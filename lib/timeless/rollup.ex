defmodule Timeless.Rollup do
  @moduledoc """
  Incremental rollup engine.

  Processes data tier-by-tier using watermarks to track progress.
  Each tier reads from its source: hourly reads raw segments,
  daily reads hourly, monthly reads daily.
  """

  use GenServer

  require Logger

  defstruct [:db, :store, :schema, :compression, :tick_count, :late_lookback, :late_every_n_ticks]

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
    db = Keyword.fetch!(opts, :db)
    store = Keyword.get(opts, :store)
    schema = Keyword.fetch!(opts, :schema)
    compression = Keyword.get(opts, :compression, :zstd)

    late_lookback = Keyword.get(opts, :late_lookback, @default_late_lookback)
    late_every = Keyword.get(opts, :late_every_n_ticks, @default_late_every_n_ticks)

    state = %__MODULE__{
      db: db,
      store: store,
      schema: schema,
      compression: compression,
      tick_count: 0,
      late_lookback: late_lookback,
      late_every_n_ticks: late_every
    }

    # Initialize watermarks for all tiers
    init_watermarks(state)

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
    watermark = get_watermark(state.db, tier.name)
    now = System.os_time(:second)
    # Only roll up complete buckets (current bucket is still accumulating)
    current_bucket = bucket_floor(now, tier.resolution_seconds)

    if watermark < current_bucket do
      {us, _} =
        :timer.tc(fn ->
          case source_for_tier(tier, state.schema.tiers) do
            :raw ->
              rollup_from_raw(tier, watermark, current_bucket, state)

            {:tier, source_tier} ->
              rollup_from_tier(tier, source_tier, watermark, current_bucket, state)
          end
        end)

      :telemetry.execute(
        [:timeless, :rollup, :complete],
        %{duration_us: us},
        %{tier: tier.name, watermark: watermark, up_to: current_bucket}
      )

      set_watermark(state.db, tier.name, current_bucket)
    end
  end

  defp rollup_from_raw(tier, watermark, up_to, state) do
    # Read raw segments that overlap with the rollup window
    {:ok, rows} =
      Timeless.DB.read(
        state.db,
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

    # Compute aggregates per series per bucket
    Timeless.DB.write_transaction(state.db, fn conn ->
      Enum.each(points_by_series, fn {series_id, series_points} ->
        series_points
        |> Enum.group_by(fn {_sid, ts, _val} -> bucket_floor(ts, tier.resolution_seconds) end)
        |> Enum.each(fn {bucket, bucket_points} ->
          values = Enum.map(bucket_points, fn {_sid, _ts, val} -> val end)
          aggs = compute_aggregates(values, tier.aggregates)
          write_rollup_row(conn, tier.table_name, series_id, bucket, aggs)
        end)
      end)
    end)
  end

  defp rollup_from_tier(tier, source_tier, watermark, up_to, state) do
    # Read from source tier's table
    {:ok, rows} =
      Timeless.DB.read(
        state.db,
        """
        SELECT series_id, bucket, avg, min, max, count, sum, last
        FROM #{source_tier.table_name}
        WHERE bucket >= ?1 AND bucket < ?2
        ORDER BY series_id, bucket
        """,
        [watermark, up_to]
      )

    # Group by series and target bucket, then re-aggregate
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
    |> then(fn grouped ->
      Timeless.DB.write_transaction(state.db, fn conn ->
        Enum.each(grouped, fn {series_id, series_rows} ->
          series_rows
          |> Enum.group_by(fn row -> bucket_floor(row.bucket, tier.resolution_seconds) end)
          |> Enum.each(fn {target_bucket, source_rows} ->
            aggs = reaggregate(source_rows, tier.aggregates)
            write_rollup_row(conn, tier.table_name, series_id, target_bucket, aggs)
          end)
        end)
      end)
    end)
  end

  # --- Late-arrival catch-up ---
  # Re-processes a lookback window behind each tier's watermark.
  # Catches data that arrived after the watermark had already advanced.
  # Idempotent: INSERT OR REPLACE safely updates existing rows.

  defp catch_up_late_arrivals(state) do
    Enum.each(state.schema.tiers, fn tier ->
      watermark = get_watermark(state.db, tier.name)

      if watermark > 0 do
        scan_from = max(watermark - state.late_lookback, 0)

        # Only scan if there's actually a window to check
        if scan_from < watermark do
          {us, _} =
            :timer.tc(fn ->
              case source_for_tier(tier, state.schema.tiers) do
                :raw ->
                  rollup_from_raw(tier, scan_from, watermark, state)

                {:tier, source_tier} ->
                  rollup_from_tier(tier, source_tier, scan_from, watermark, state)
              end
            end)

          :telemetry.execute(
            [:timeless, :rollup, :late_catch_up],
            %{duration_us: us},
            %{tier: tier.name, scan_from: scan_from, watermark: watermark}
          )
        end
      end
    end)
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

  # --- SQLite Helpers ---

  defp write_rollup_row(conn, table_name, series_id, bucket, aggs) do
    Timeless.DB.execute(
      conn,
      """
      INSERT OR REPLACE INTO #{table_name} (series_id, bucket, avg, min, max, count, sum, last)
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
      """,
      [series_id, bucket, aggs.avg, aggs.min, aggs.max, aggs.count, aggs.sum, aggs.last]
    )
  end

  defp init_watermarks(state) do
    Enum.each(state.schema.tiers, fn tier ->
      Timeless.DB.write(
        state.db,
        "INSERT OR IGNORE INTO _watermarks (tier, last_bucket) VALUES (?1, 0)",
        [to_string(tier.name)]
      )
    end)
  end

  defp get_watermark(db, tier_name) do
    {:ok, rows} =
      Timeless.DB.read(db, "SELECT last_bucket FROM _watermarks WHERE tier = ?1", [
        to_string(tier_name)
      ])

    case rows do
      [[bucket]] -> bucket
      [] -> 0
    end
  end

  defp set_watermark(db, tier_name, bucket) do
    Timeless.DB.write(
      db,
      "INSERT OR REPLACE INTO _watermarks (tier, last_bucket) VALUES (?1, ?2)",
      [to_string(tier_name), bucket]
    )
  end

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
