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
        :ok -> :ok
        {:ok, _} -> :ok
        {:error, e} -> Logger.warning("Shard rollup failed for #{tier.name} on #{builder}: #{inspect(e)}")
      end
    end
  end

  defp rollup_shard_from_raw(tier, builder, watermark, up_to, state) do
    {:ok, rows} =
      Timeless.SegmentBuilder.read_raw_for_rollup(builder, watermark, up_to)

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

    # Prepare tier entries (lock-free reads for merge)
    if points_by_series != %{} do
      entries = collect_tier_entries(builder, tier, points_by_series)

      if entries != [] do
        Timeless.SegmentBuilder.write_tier_batch(builder, tier.name, entries)
      end
    end

    # Advance watermark
    Timeless.SegmentBuilder.write_watermark(builder, tier.name, up_to)
  end

  defp rollup_shard_from_tier(tier, source_tier, builder, watermark, up_to, _state) do
    # Read compressed chunks from source tier covering [watermark, up_to)
    {:ok, rows} =
      Timeless.SegmentBuilder.read_tier_for_rollup(builder, source_tier.name, watermark, up_to)

    # Decode chunks and filter buckets to range, group by series
    grouped =
      rows
      |> Enum.flat_map(fn [series_id, blob] ->
        {_aggs, buckets} = Timeless.TierChunk.decode(blob)

        buckets
        |> Enum.filter(fn b -> b.bucket >= watermark and b.bucket < up_to end)
        |> Enum.map(fn b -> Map.put(b, :series_id, series_id) end)
      end)
      |> Enum.group_by(& &1.series_id)

    if grouped != %{} do
      entries =
        Enum.flat_map(grouped, fn {series_id, series_rows} ->
          # Re-aggregate into target tier resolution, then group by chunk boundary
          tier_buckets =
            series_rows
            |> Enum.group_by(fn row -> bucket_floor(row.bucket, tier.resolution_seconds) end)
            |> Enum.map(fn {target_bucket, source_rows} ->
              aggs = reaggregate(source_rows, tier.aggregates)
              Map.put(aggs, :bucket, target_bucket)
            end)

          prepare_tier_entries(builder, tier, series_id, tier_buckets)
        end)

      if entries != [] do
        Timeless.SegmentBuilder.write_tier_batch(builder, tier.name, entries)
      end
    end

    # Advance watermark
    Timeless.SegmentBuilder.write_watermark(builder, tier.name, up_to)
  end

  defp collect_tier_entries(builder, tier, points_by_series) do
    Enum.flat_map(points_by_series, fn {series_id, series_points} ->
      tier_buckets =
        series_points
        |> Enum.group_by(fn {_sid, ts, _val} -> bucket_floor(ts, tier.resolution_seconds) end)
        |> Enum.map(fn {bucket, bucket_points} ->
          values = Enum.map(bucket_points, fn {_sid, _ts, val} -> val end)
          aggs = compute_aggregates(values, tier.aggregates)
          Map.put(aggs, :bucket, bucket)
        end)

      prepare_tier_entries(builder, tier, series_id, tier_buckets)
    end)
  end

  defp prepare_tier_entries(builder, tier, series_id, tier_buckets) do
    chunk_secs = tier.chunk_seconds || Timeless.Schema.chunk_seconds(nil, tier.resolution_seconds)

    # Group buckets by chunk boundary
    chunks =
      Enum.group_by(tier_buckets, fn b ->
        div(b.bucket, chunk_secs) * chunk_secs
      end)

    Enum.map(chunks, fn {chunk_start, new_buckets} ->
      chunk_end = chunk_start + chunk_secs

      # Read existing chunk (lock-free via ETS)
      existing_blob =
        Timeless.SegmentBuilder.read_tier_chunk_for_merge(builder, tier.name, series_id, chunk_start)

      # Merge new buckets into existing (or create new)
      merged_blob = Timeless.TierChunk.merge(existing_blob, new_buckets, tier.aggregates)
      bucket_count = Timeless.TierChunk.bucket_count(merged_blob)

      {series_id, chunk_start, chunk_end, bucket_count, merged_blob}
    end)
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
    Timeless.SegmentBuilder.read_watermark(builder, tier_name)
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
