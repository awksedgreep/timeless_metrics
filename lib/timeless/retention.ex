defmodule Timeless.Retention do
  @moduledoc """
  Tier-aware retention enforcer.

  Periodically deletes expired raw segments (from shard DBs) and rollup rows
  (from main DB) based on configured retention periods. Runs incremental
  vacuum to reclaim space.
  """

  use GenServer

  require Logger

  defstruct [:db, :store, :schema, :vacuum_counter]

  @vacuum_every 24  # Run vacuum every N retention cycles

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Force retention enforcement now."
  def enforce(retention) do
    GenServer.call(retention, :enforce, :infinity)
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
    shard_count = :persistent_term.get({Timeless, state.store, :shard_count})

    # 1. Drop expired raw segments from all shard DBs
    if state.schema.raw_retention_seconds != :forever do
      raw_cutoff = now - state.schema.raw_retention_seconds

      for i <- 0..(shard_count - 1) do
        builder = :"#{state.store}_builder_#{i}"
        Timeless.SegmentBuilder.delete_shard(
          builder,
          "DELETE FROM raw_segments WHERE end_time < ?1",
          [raw_cutoff]
        )
      end
    end

    # 2. Drop expired rollup rows per tier (from all shard DBs)
    Enum.each(state.schema.tiers, fn tier ->
      if tier.retention_seconds != :forever do
        cutoff = now - tier.retention_seconds

        for i <- 0..(shard_count - 1) do
          builder = :"#{state.store}_builder_#{i}"
          Timeless.SegmentBuilder.delete_shard(
            builder,
            "DELETE FROM #{tier.table_name} WHERE bucket < ?1",
            [cutoff]
          )
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
      Timeless.DB.write(state.db, "PRAGMA incremental_vacuum(1000)", [])
    end

    %{state | vacuum_counter: counter}
  end

  defp cleanup_orphaned_series(state, shard_count) do
    # Gather all series_ids with data across all shards (raw + tier tables)
    all_active_ids =
      for i <- 0..(shard_count - 1) do
        builder = :"#{state.store}_builder_#{i}"

        # Raw segments
        {:ok, raw_rows} = Timeless.SegmentBuilder.read_shard(
          builder,
          "SELECT DISTINCT series_id FROM raw_segments",
          []
        )
        raw_ids = Enum.map(raw_rows, fn [id] -> id end)

        # Tier tables
        tier_ids =
          Enum.flat_map(state.schema.tiers, fn tier ->
            {:ok, tier_rows} = Timeless.SegmentBuilder.read_shard(
              builder,
              "SELECT DISTINCT series_id FROM #{tier.table_name}",
              []
            )
            Enum.map(tier_rows, fn [id] -> id end)
          end)

        raw_ids ++ tier_ids
      end
      |> List.flatten()
      |> Enum.uniq()

    if all_active_ids == [] do
      Timeless.DB.write(state.db, "DELETE FROM series", [])
    else
      values =
        all_active_ids
        |> Enum.map(fn id -> "SELECT #{id}" end)
        |> Enum.join(" UNION ALL ")

      sql = "DELETE FROM series WHERE id NOT IN (#{values})"
      Timeless.DB.write(state.db, sql, [])
    end
  end

  defp cleanup_orphaned_alerts(state) do
    {:ok, rules} = Timeless.Alert.list_rules(state.db)

    Enum.each(rules, fn rule ->
      {:ok, series_rows} =
        Timeless.DB.read(
          state.db,
          "SELECT COUNT(*) FROM series WHERE metric_name = ?1",
          [rule.metric]
        )

      case series_rows do
        [[0]] ->
          Logger.info("Deleting orphaned alert rule #{rule.id} (#{rule.name}): metric '#{rule.metric}' has no series")
          Timeless.Alert.delete_rule(state.db, rule.id)

        _ ->
          :ok
      end
    end)
  end

  defp schedule_tick(interval) do
    Process.send_after(self(), :tick, interval)
  end
end
