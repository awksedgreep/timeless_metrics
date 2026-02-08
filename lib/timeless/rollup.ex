defmodule Timeless.Rollup do
  @moduledoc """
  Sharded rollup engine.

  Processes data tier-by-tier with parallel per-shard execution.
  Each shard maintains its own tier tables and watermarks.
  The Rollup GenServer acts as a coordinator, spawning parallel
  tasks for each shard.
  """

  use GenServer

  require Logger

  defstruct [:store, :schema, :compression, :tick_count, :late_lookback, :late_every_n_ticks]

  @default_late_lookback 7_200       # 2 hours
  @default_late_every_n_ticks 3      # every 3rd tick (15 min at default interval)

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Force a rollup of a specific tier (or all tiers)."
  def run(rollup, tier \\ :all) do
    GenServer.call(rollup, {:run, tier}, :infinity)
  end

  @doc "Force a late-arrival catch-up scan."
  def catch_up(rollup) do
    GenServer.call(rollup, :catch_up, :infinity)
  end

  # --- Server ---

  @impl true
  def init(opts) do
    store = Keyword.get(opts, :store)
    schema = Keyword.fetch!(opts, :schema)
    compression = Keyword.get(opts, :compression, :zstd)

    late_lookback = Keyword.get(opts, :late_lookback, @default_late_lookback)
    late_every = Keyword.get(opts, :late_every_n_ticks, @default_late_every_n_ticks)

    state = %__MODULE__{
      store: store,
      schema: schema,
      compression: compression,
      tick_count: 0,
      late_lookback: late_lookback,
      late_every_n_ticks: late_every
    }

    # Watermarks are now initialized by each SegmentBuilder

    schedule_tick(schema.rollup_interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    run_all_tiers(state)
    tick = state.tick_count + 1

    state =
      if rem(tick, state.late_every_n_ticks) == 0 do
        catch_up_late_arrivals(state)
        %{state | tick_count: tick}
      else
        %{state | tick_count: tick}
      end

    # Evaluate alert rules after rollup
    if state.store do
      try do
        Timeless.Alert.evaluate(state.store)
      rescue
        e -> Logger.warning("Alert evaluation failed: #{inspect(e)}")
      end
    end

    schedule_tick(state.schema.rollup_interval)
    {:noreply, state}
  end

  @impl true
  def handle_call(:catch_up, _from, state) do
    catch_up_late_arrivals(state)
    {:reply, :ok, state}
  end

  def handle_call({:run, :all}, _from, state) do
    run_all_tiers(state)
    {:reply, :ok, state}
  end

  def handle_call({:run, tier_name}, _from, state) do
    tier = Enum.find(state.schema.tiers, &(&1.name == tier_name))

    if tier do
      run_tier(tier, state)
      {:reply, :ok, state}
    else
      {:reply, {:error, :unknown_tier}, state}
    end
  end

  # --- Core Logic ---

  defp run_all_tiers(state) do
    Enum.each(state.schema.tiers, fn tier ->
      run_tier(tier, state)
    end)
  end

  defp run_tier(tier, state) do
    shard_count = :persistent_term.get({Timeless, state.store, :shard_count})
    source = source_for_tier(tier, state.schema.tiers)

    {us, _} =
      :timer.tc(fn ->
        tasks =
          for i <- 0..(shard_count - 1) do
            builder = :"#{state.store}_builder_#{i}"
            Task.async(fn -> run_tier_on_shard(tier, source, builder, state) end)
          end

        Task.await_many(tasks, :infinity)
      end)

    :telemetry.execute(
      [:timeless, :rollup, :complete],
      %{duration_us: us},
      %{tier: tier.name}
    )
  end

  defp run_tier_on_shard(tier, source, builder, state) do
    watermark = get_shard_watermark(builder, tier.name)
    now = System.os_time(:second)
    current_bucket = bucket_floor(now, tier.resolution_seconds)

    if watermark < current_bucket do
      result =
        case source do
          :raw ->
            rollup_shard_from_raw(tier, builder, watermark, current_bucket, state)

          {:tier, source_tier} ->
            rollup_shard_from_tier(tier, source_tier, builder, watermark, current_bucket, state)
        end

      case result do
        {:ok, _} -> :ok
        {:error, e} -> Logger.warning("Shard rollup failed for #{tier.name} on #{builder}: #{inspect(e)}")
      end
    end
  end

  defp rollup_shard_from_raw(tier, builder, watermark, up_to, state) do
    {:ok, rows} =
      Timeless.SegmentBuilder.read_shard(
        builder,
        """
        SELECT series_id, start_time, end_time, data
        FROM raw_segments
        WHERE end_time > ?1 AND start_time < ?2
        ORDER BY series_id, start_time
        """,
        [watermark, up_to]
      )

    # Decompress all segments and gather points
    points_by_series =
      rows
      |> Enum.flat_map(fn [series_id, _start, _end, blob] ->
        case GorillaStream.decompress(blob, compression: state.compression) do
          {:ok, points} ->
            Enum.map(points, fn {ts, val} -> {series_id, ts, val} end)

          {:error, _} ->
            []
        end
      end)
      |> Enum.filter(fn {_sid, ts, _val} -> ts >= watermark and ts < up_to end)
      |> Enum.group_by(&elem(&1, 0))

    # Write tier rows + watermark atomically
    Timeless.SegmentBuilder.write_transaction_shard(builder, fn conn ->
      if points_by_series != %{} do
        write_tier_rows(conn, tier, points_by_series)
      end

      # Always advance watermark
      Timeless.SegmentBuilder.execute(
        conn,
        "INSERT OR REPLACE INTO _watermarks (tier, last_bucket) VALUES (?1, ?2)",
        [to_string(tier.name), up_to]
      )
    end)
  end

  defp rollup_shard_from_tier(tier, source_tier, builder, watermark, up_to, _state) do
    {:ok, rows} =
      Timeless.SegmentBuilder.read_shard(
        builder,
        """
        SELECT series_id, bucket, avg, min, max, count, sum, last
        FROM #{source_tier.table_name}
        WHERE bucket >= ?1 AND bucket < ?2
        ORDER BY series_id, bucket
        """,
        [watermark, up_to]
      )

    grouped =
      rows
      |> Enum.map(fn [series_id, bucket, avg, min, max, count, sum, last] ->
        %{
          series_id: series_id,
          bucket: bucket,
          avg: avg,
          min: min,
          max: max,
          count: count,
          sum: sum,
          last: last
        }
      end)
      |> Enum.group_by(& &1.series_id)

    Timeless.SegmentBuilder.write_transaction_shard(builder, fn conn ->
      if grouped != %{} do
        insert_sql = "INSERT OR REPLACE INTO #{tier.table_name} (series_id, bucket, avg, min, max, count, sum, last) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
        {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, insert_sql)

        try do
          Enum.each(grouped, fn {series_id, series_rows} ->
            series_rows
            |> Enum.group_by(fn row -> bucket_floor(row.bucket, tier.resolution_seconds) end)
            |> Enum.each(fn {target_bucket, source_rows} ->
              aggs = reaggregate(source_rows, tier.aggregates)
              :ok = Exqlite.Sqlite3.bind(stmt, [series_id, target_bucket, aggs.avg, aggs.min, aggs.max, aggs.count, aggs.sum, aggs.last])
              :done = Exqlite.Sqlite3.step(conn, stmt)
              :ok = Exqlite.Sqlite3.reset(stmt)
            end)
          end)
        after
          Exqlite.Sqlite3.release(conn, stmt)
        end
      end

      # Always advance watermark
      Timeless.SegmentBuilder.execute(
        conn,
        "INSERT OR REPLACE INTO _watermarks (tier, last_bucket) VALUES (?1, ?2)",
        [to_string(tier.name), up_to]
      )
    end)
  end

  defp write_tier_rows(conn, tier, points_by_series) do
    insert_sql = "INSERT OR REPLACE INTO #{tier.table_name} (series_id, bucket, avg, min, max, count, sum, last) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
    {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, insert_sql)

    try do
      Enum.each(points_by_series, fn {series_id, series_points} ->
        series_points
        |> Enum.group_by(fn {_sid, ts, _val} -> bucket_floor(ts, tier.resolution_seconds) end)
        |> Enum.each(fn {bucket, bucket_points} ->
          values = Enum.map(bucket_points, fn {_sid, _ts, val} -> val end)
          aggs = compute_aggregates(values, tier.aggregates)
          :ok = Exqlite.Sqlite3.bind(stmt, [series_id, bucket, aggs.avg, aggs.min, aggs.max, aggs.count, aggs.sum, aggs.last])
          :done = Exqlite.Sqlite3.step(conn, stmt)
          :ok = Exqlite.Sqlite3.reset(stmt)
        end)
      end)
    after
      Exqlite.Sqlite3.release(conn, stmt)
    end
  end

  # --- Late-arrival catch-up ---
  # Re-processes a lookback window behind each tier's watermark per shard.

  defp catch_up_late_arrivals(state) do
    shard_count = :persistent_term.get({Timeless, state.store, :shard_count})

    Enum.each(state.schema.tiers, fn tier ->
      source = source_for_tier(tier, state.schema.tiers)

      tasks =
        for i <- 0..(shard_count - 1) do
          builder = :"#{state.store}_builder_#{i}"
          Task.async(fn -> catch_up_shard(tier, source, builder, state) end)
        end

      Task.await_many(tasks, :infinity)
    end)
  end

  defp catch_up_shard(tier, source, builder, state) do
    watermark = get_shard_watermark(builder, tier.name)

    if watermark > 0 do
      scan_from = max(watermark - state.late_lookback, 0)

      if scan_from < watermark do
        {us, _} =
          :timer.tc(fn ->
            case source do
              :raw ->
                rollup_shard_from_raw(tier, builder, scan_from, watermark, state)

              {:tier, source_tier} ->
                rollup_shard_from_tier(tier, source_tier, builder, scan_from, watermark, state)
            end
          end)

        :telemetry.execute(
          [:timeless, :rollup, :late_catch_up],
          %{duration_us: us},
          %{tier: tier.name}
        )
      end
    end
  end

  # --- Shard watermark helpers ---

  defp get_shard_watermark(builder, tier_name) do
    {:ok, rows} =
      Timeless.SegmentBuilder.read_shard(
        builder,
        "SELECT last_bucket FROM _watermarks WHERE tier = ?1",
        [to_string(tier_name)]
      )

    case rows do
      [[w]] -> w
      [] -> 0
    end
  end

  # --- Aggregation ---

  defp compute_aggregates(values, _aggregates) do
    count = length(values)

    %{
      avg: Enum.sum(values) / count,
      min: Enum.min(values),
      max: Enum.max(values),
      count: count,
      sum: Enum.sum(values),
      last: List.last(values)
    }
  end

  defp reaggregate(source_rows, _aggregates) do
    total_count = Enum.sum(Enum.map(source_rows, & &1.count))
    total_sum = Enum.sum(Enum.map(source_rows, & &1.sum))

    %{
      avg: if(total_count > 0, do: total_sum / total_count, else: 0.0),
      min: source_rows |> Enum.map(& &1.min) |> Enum.min(),
      max: source_rows |> Enum.map(& &1.max) |> Enum.max(),
      count: total_count,
      sum: total_sum,
      last: source_rows |> Enum.max_by(& &1.bucket) |> Map.get(:last)
    }
  end

  # --- Helpers ---

  defp bucket_floor(timestamp, resolution) do
    div(timestamp, resolution) * resolution
  end

  defp source_for_tier(tier, all_tiers) do
    idx = Enum.find_index(all_tiers, &(&1.name == tier.name))

    if idx == 0 do
      :raw
    else
      {:tier, Enum.at(all_tiers, idx - 1)}
    end
  end

  defp schedule_tick(interval) do
    Process.send_after(self(), :tick, interval)
  end
end
