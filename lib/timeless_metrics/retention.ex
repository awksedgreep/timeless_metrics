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
    # Gather all series_ids with data across all shards (raw + tier tables)
    all_active_ids =
      for i <- 0..(shard_count - 1) do
        builder = :"#{state.store}_builder_#{i}"

        # Raw segments
        {:ok, raw_rows} = TimelessMetrics.SegmentBuilder.raw_series_ids(builder)
        raw_ids = Enum.map(raw_rows, fn [id] -> id end)

        # Tier tables (from ShardStore ETS)
        tier_ids =
          Enum.flat_map(state.schema.tiers, fn tier ->
            {:ok, tier_rows} = TimelessMetrics.SegmentBuilder.read_tier_series_ids(builder, tier.name)
            Enum.map(tier_rows, fn [id] -> id end)
          end)

        raw_ids ++ tier_ids
      end
      |> List.flatten()
      |> Enum.uniq()

    if all_active_ids == [] do
      TimelessMetrics.DB.write(state.db, "DELETE FROM series", [])
    else
      values =
        all_active_ids
        |> Enum.map(fn id -> "SELECT #{id}" end)
        |> Enum.join(" UNION ALL ")

      sql = "DELETE FROM series WHERE id NOT IN (#{values})"
      TimelessMetrics.DB.write(state.db, sql, [])
    end
  end

  defp cleanup_orphaned_alerts(state) do
    {:ok, rules} = TimelessMetrics.Alert.list_rules(state.db)

    Enum.each(rules, fn rule ->
      {:ok, series_rows} =
        TimelessMetrics.DB.read(
          state.db,
          "SELECT COUNT(*) FROM series WHERE metric_name = ?1",
          [rule.metric]
        )

      case series_rows do
        [[0]] ->
          Logger.info("Deleting orphaned alert rule #{rule.id} (#{rule.name}): metric '#{rule.metric}' has no series")
          TimelessMetrics.Alert.delete_rule(state.db, rule.id)

        _ ->
          :ok
      end
    end)
  end

  defp schedule_tick(interval) do
    Process.send_after(self(), :tick, interval)
  end
end
