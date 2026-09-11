defmodule TimelessMetrics.Retention do
  @moduledoc """
  Tier-aware retention enforcer.

  Periodically deletes expired raw segments (from shard DBs) and rollup rows
  (from main DB) based on configured retention periods. Runs incremental
  vacuum to reclaim space.
  """

  use GenServer

  require Logger

  defstruct [:db, :store, :schema, :vacuum_counter]

  # Run vacuum every N retention cycles
  @vacuum_every 24

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Force retention enforcement now."
  def enforce(retention) do
    TimelessMetrics.Call.maintenance(retention, :enforce)
  end

  # --- Server ---

  @impl true
  def init(opts) do
    db = Keyword.fetch!(opts, :db)
    store = Keyword.fetch!(opts, :store)
    schema = Keyword.fetch!(opts, :schema)

    state = %__MODULE__{db: db, store: store, schema: schema, vacuum_counter: 0}

    schedule_tick(schema.retention_interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    new_state = do_enforce(state)
    schedule_tick(state.schema.retention_interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_call(:enforce, _from, state) do
    new_state = do_enforce(state)
    {:reply, :ok, new_state}
  end

  # --- Core Logic ---

  defp do_enforce(state) do
    now = System.os_time(:second)
    shard_count = :persistent_term.get({TimelessMetrics, state.store, :shard_count})

    # 1. Drop expired raw segments from all shards
    if state.schema.raw_retention_seconds != :forever do
      raw_cutoff = now - state.schema.raw_retention_seconds

      for i <- 0..(shard_count - 1) do
        builder = :"#{state.store}_builder_#{i}"
        TimelessMetrics.SegmentBuilder.delete_raw_before(builder, raw_cutoff)
      end
    end

    # 2. Drop expired compressed tier chunks (fully expired only) + compact
    Enum.each(state.schema.tiers, fn tier ->
      if tier.retention_seconds != :forever do
        cutoff = now - tier.retention_seconds

        for i <- 0..(shard_count - 1) do
          builder = :"#{state.store}_builder_#{i}"
          TimelessMetrics.SegmentBuilder.delete_tier_before(builder, tier.name, cutoff)

          # Compact if deletion created significant dead space
          TimelessMetrics.SegmentBuilder.compact_tier(builder, tier.name)
        end
      end
    end)

    # 3. Clean orphaned series (no data in any table)
    cleanup_orphaned_series(state, shard_count)

    # 4. Clean orphaned alert rules (metric no longer has any series)
    cleanup_orphaned_alerts(state)

    # 5. Periodic vacuum on main DB
    counter = state.vacuum_counter + 1

    if rem(counter, @vacuum_every) == 0 do
      TimelessMetrics.DB.write(state.db, "PRAGMA incremental_vacuum(1000)", [])
    end

    %{state | vacuum_counter: counter}
  end

  defp cleanup_orphaned_series(state, shard_count) do
    with {:ok, all_active_ids} <- collect_active_ids(state, shard_count),
         :ok <- validate_active_ids(all_active_ids) do
      persist_active_ids(state.db, Enum.uniq(all_active_ids))
    else
      {:error, reason} = error ->
        Logger.warning("Skipping orphaned-series cleanup: #{inspect(reason)}")
        error
    end
  end

  defp collect_active_ids(state, shard_count) do
    Enum.reduce_while(0..(shard_count - 1), {:ok, []}, fn i, {:ok, acc} ->
      builder = :"#{state.store}_builder_#{i}"

      with {:ok, raw_rows} <- TimelessMetrics.SegmentBuilder.raw_series_ids(builder),
           {:ok, tier_ids} <- collect_tier_ids(builder, state.schema.tiers) do
        raw_ids = Enum.map(raw_rows, &series_id_from_row/1)
        {:cont, {:ok, :lists.reverse(raw_ids ++ tier_ids, acc)}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _} = error -> error
    end
  end

  defp collect_tier_ids(builder, tiers) do
    Enum.reduce_while(tiers, {:ok, []}, fn tier, {:ok, acc} ->
      case TimelessMetrics.SegmentBuilder.read_tier_series_ids(builder, tier.name) do
        {:ok, rows} ->
          ids = Enum.map(rows, &series_id_from_row/1)
          {:cont, {:ok, :lists.reverse(ids, acc)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _} = error -> error
    end
  end

  defp series_id_from_row([id]), do: id
  defp series_id_from_row(row), do: {:invalid_series_row, row}

  defp validate_active_ids(ids) do
    case Enum.find(ids, &(not is_integer(&1) or &1 < 0)) do
      nil -> :ok
      invalid -> {:error, {:invalid_series_id, invalid}}
    end
  end

  defp persist_active_ids(db, active_ids) do
    TimelessMetrics.DB.write_transaction(db, fn conn ->
      with {:ok, _} <-
             TimelessMetrics.DB.execute(
               conn,
               "CREATE TEMP TABLE IF NOT EXISTS _active_retention_series " <>
                 "(id INTEGER PRIMARY KEY) WITHOUT ROWID",
               []
             ),
           {:ok, _} <-
             TimelessMetrics.DB.execute(conn, "DELETE FROM _active_retention_series", []),
           :ok <- insert_active_id_chunks(conn, active_ids),
           {:ok, _} <-
             TimelessMetrics.DB.execute(
               conn,
               "DELETE FROM series WHERE NOT EXISTS " <>
                 "(SELECT 1 FROM _active_retention_series active WHERE active.id = series.id)",
               []
             ) do
        :ok
      end
    end)
  end

  defp insert_active_id_chunks(conn, active_ids) do
    active_ids
    |> Enum.chunk_every(500)
    |> Enum.reduce_while(:ok, fn ids, :ok ->
      placeholders = Enum.map_join(1..length(ids), ",", &"(?#{&1})")

      case TimelessMetrics.DB.execute(
             conn,
             "INSERT OR IGNORE INTO _active_retention_series(id) VALUES #{placeholders}",
             ids
           ) do
        {:ok, _} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp cleanup_orphaned_alerts(state) do
    with {:ok, rules} <- TimelessMetrics.Alert.list_rules(state.db),
         {:ok, metric_rows} <-
           TimelessMetrics.DB.read(
             state.db,
             "SELECT metric_name, COUNT(*) FROM series GROUP BY metric_name",
             []
           ) do
      active_metrics = MapSet.new(metric_rows, fn [metric, _count] -> metric end)

      Enum.each(rules, fn rule ->
        if not MapSet.member?(active_metrics, rule.metric) do
          Logger.info(
            "Deleting orphaned alert rule #{rule.id} (#{rule.name}): metric '#{rule.metric}' has no series"
          )

          TimelessMetrics.Alert.delete_rule(state.db, rule.id)
        end
      end)
    else
      {:error, reason} -> Logger.warning("Skipping orphaned-alert cleanup: #{inspect(reason)}")
    end
  end

  defp schedule_tick(interval) do
    Process.send_after(self(), :tick, interval)
  end
end
