defmodule TimelessMetrics.Query do
  @moduledoc """
  Query engine with automatic tier selection.

  For raw queries, reads gorilla-compressed segments from shard DBs.
  For aggregate queries, reads from rollup tiers when the time range
  falls outside raw retention, or when a matching tier resolution exists.
  Stitches results across tiers for queries spanning multiple windows.
  """

  @doc """
  Query raw points for a series within a time range.

  Reads from the shard DB that owns this series_id.

  Returns `{:ok, [{timestamp, value}, ...]}` sorted by timestamp.
  """
  def raw(store, series_id, opts \\ []) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))
    compression = Keyword.get(opts, :compression, :zstd)

    builder = builder_for_series(store, series_id)

    {:ok, rows} =
      TimelessMetrics.SegmentBuilder.read_raw_segments(builder, series_id, from, to)

    {us, points} =
      :timer.tc(fn ->
        rows
        |> Enum.map(fn [blob, _start, _end] ->
          case GorillaStream.decompress(blob, compression: compression) do
            {:ok, pts} -> Enum.filter(pts, fn {ts, _} -> ts >= from and ts <= to end)
            {:error, _} -> []
          end
        end)
        |> :lists.merge()
      end)

    :telemetry.execute(
      [:timeless_metrics, :query, :raw],
      %{duration_us: us, point_count: length(points), segment_count: length(rows)},
      %{series_id: series_id}
    )

    {:ok, points}
  end

  @doc """
  Query with aggregation. Automatically selects the best tier.

  When a schema is provided, the query router will read from rollup
  tables if the data is available there, falling back to raw segments.

  Returns `{:ok, [{bucket_timestamp, aggregate_value}, ...]}`.
  """
  def aggregate(store, series_id, opts \\ []) do
    bucket_seconds = bucket_to_seconds(Keyword.fetch!(opts, :bucket))
    agg_fn = Keyword.fetch!(opts, :aggregate)
    schema = Keyword.get(opts, :schema)

    # Try to read from a rollup tier if schema is available
    if schema do
      aggregate_with_tiers(store, series_id, opts, schema, bucket_seconds, agg_fn)
    else
      aggregate_from_raw(store, series_id, opts, bucket_seconds, agg_fn)
    end
  end

  @doc """
  Read pre-computed rollup data directly from a tier table.

  Returns `{:ok, [%{bucket: ts, avg: v, min: v, max: v, count: n, sum: v, last: v}, ...]}`.
  """
  def read_tier(store, tier_name, series_id, opts \\ []) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))
    builder = builder_for_series(store, series_id)

    {:ok, rows} =
      TimelessMetrics.SegmentBuilder.read_tier_chunks(builder, tier_name, series_id, from, to)

    results =
      rows
      |> Enum.flat_map(fn [blob] ->
        {_aggs, buckets} = TimelessMetrics.TierChunk.decode(blob)
        Enum.filter(buckets, fn b -> b.bucket >= from and b.bucket < to end)
      end)

    {:ok, results}
  end

  @doc """
  Query the latest value for a series.
  Checks raw segments first (from shard DB), then falls back to the finest rollup tier.
  """
  def latest(store, series_id, opts \\ []) do
    compression = Keyword.get(opts, :compression, :zstd)
    schema = Keyword.get(opts, :schema)

    builder = builder_for_series(store, series_id)

    # Try raw first (from shard storage)
    {:ok, rows} =
      TimelessMetrics.SegmentBuilder.read_raw_latest(builder, series_id)

    case rows do
      [[blob]] ->
        case GorillaStream.decompress(blob, compression: compression) do
          {:ok, points} ->
            {ts, val} = List.last(Enum.sort_by(points, &elem(&1, 0)))
            {:ok, {ts, val}}

          {:error, reason} ->
            {:error, reason}
        end

      [] ->
        # Fall back to finest rollup tier (on shard DB)
        if schema && schema.tiers != [] do
          finest_tier = List.first(schema.tiers)
          latest_from_tier(store, finest_tier.name, series_id)
        else
          {:ok, nil}
        end
    end
  end

  # --- Shard routing ---

  defp builder_for_series(store, series_id) do
    shard_count = :persistent_term.get({TimelessMetrics, store, :shard_count})
    shard_idx = rem(abs(series_id), shard_count)
    :"#{store}_builder_#{shard_idx}"
  end

  # --- Tier-aware aggregation with watermark-based stitching ---

  defp aggregate_with_tiers(store, series_id, opts, schema, bucket_seconds, agg_fn) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))

    # No tiers configured — use raw for everything
    if schema.tiers == [] do
      aggregate_from_raw(store, series_id, opts, bucket_seconds, agg_fn)
    else
      # Find best tier whose resolution is finer than or equal to requested bucket
      tier =
        schema.tiers
        |> Enum.filter(fn t -> t.resolution_seconds <= bucket_seconds end)
        |> Enum.max_by(& &1.resolution_seconds, fn -> List.first(schema.tiers) end)

      # Read watermark from the series' shard
      builder = builder_for_series(store, series_id)
      watermark = get_shard_watermark(builder, tier.name)

      cond do
        # Rollup hasn't run yet or no rollup data covers this range — use raw
        watermark == 0 or from >= watermark ->
          aggregate_from_raw(store, series_id, opts, bucket_seconds, agg_fn)

        # Entire query range is covered by rollup data
        to <= watermark ->
          aggregate_from_rollup_tier(store, tier, series_id, from, to, bucket_seconds, agg_fn)

        # Query spans the boundary — stitch rollup + raw
        true ->
          {:ok, rollup_results} =
            aggregate_from_rollup_tier(
              store,
              tier,
              series_id,
              from,
              watermark,
              bucket_seconds,
              agg_fn
            )

          {:ok, raw_results} =
            aggregate_from_raw(
              store,
              series_id,
              [from: watermark, to: to],
              bucket_seconds,
              agg_fn
            )

          # Merge: raw wins for any overlapping buckets (more precise)
          raw_buckets = MapSet.new(raw_results, &elem(&1, 0))

          merged =
            (Enum.reject(rollup_results, fn {b, _} -> MapSet.member?(raw_buckets, b) end) ++
               raw_results)
            |> Enum.sort_by(&elem(&1, 0))

          {:ok, merged}
      end
    end
  end

  defp get_shard_watermark(builder, tier_name) do
    TimelessMetrics.SegmentBuilder.read_watermark(builder, tier_name)
  end

  defp aggregate_from_raw(store, series_id, opts, bucket_seconds, agg_fn) do
    {:ok, points} = raw(store, series_id, opts)

    bucketed =
      points
      |> Enum.group_by(fn {ts, _val} -> div(ts, bucket_seconds) * bucket_seconds end)
      |> Enum.map(fn {bucket, bucket_points} ->
        values = Enum.map(bucket_points, &elem(&1, 1))
        {bucket, compute_aggregate(agg_fn, values, bucket_points)}
      end)
      |> Enum.sort_by(&elem(&1, 0))

    {:ok, bucketed}
  end

  defp aggregate_from_rollup_tier(store, tier, series_id, from, to, bucket_seconds, agg_fn) do
    builder = builder_for_series(store, series_id)

    {:ok, chunk_rows} =
      TimelessMetrics.SegmentBuilder.read_tier_chunks(builder, tier.name, series_id, from, to)

    # Decode chunks and filter to requested range
    tier_rows =
      chunk_rows
      |> Enum.flat_map(fn [blob] ->
        {_aggs, buckets} = TimelessMetrics.TierChunk.decode(blob)
        Enum.filter(buckets, fn b -> b.bucket >= from and b.bucket < to end)
      end)

    # If tier resolution matches bucket size, return directly
    if tier.resolution_seconds == bucket_seconds do
      results =
        tier_rows
        |> Enum.map(fn row ->
          value =
            case agg_fn do
              :avg -> row.avg
              :min -> row.min
              :max -> row.max
              :count -> row.count
              :sum -> row.sum
              :last -> row.last
              :first -> row.avg
              :rate -> nil
            end

          {row.bucket, value, %{last: row.last, count: row.count}}
        end)

      results =
        if agg_fn == :rate do
          compute_tier_rate(results, tier.resolution_seconds)
        else
          Enum.map(results, fn {bucket, value, _meta} -> {bucket, value} end)
        end

      {:ok, results}
    else
      # Re-bucket: group tier rows into larger buckets
      results =
        tier_rows
        |> Enum.group_by(fn row -> div(row.bucket, bucket_seconds) * bucket_seconds end)
        |> Enum.map(fn {target_bucket, group} ->
          value = reaggregate_for(agg_fn, group)
          {target_bucket, value}
        end)
        |> Enum.sort_by(&elem(&1, 0))

      {:ok, results}
    end
  end

  defp reaggregate_for(:avg, rows) do
    total_count = Enum.sum(Enum.map(rows, & &1.count))
    total_sum = Enum.sum(Enum.map(rows, & &1.sum))
    if total_count > 0, do: total_sum / total_count, else: 0.0
  end

  defp reaggregate_for(:min, rows), do: rows |> Enum.map(& &1.min) |> Enum.min()
  defp reaggregate_for(:max, rows), do: rows |> Enum.map(& &1.max) |> Enum.max()
  defp reaggregate_for(:sum, rows), do: rows |> Enum.map(& &1.sum) |> Enum.sum()
  defp reaggregate_for(:count, rows), do: rows |> Enum.map(& &1.count) |> Enum.sum()
  defp reaggregate_for(:last, rows), do: rows |> Enum.max_by(& &1.bucket) |> Map.get(:last)
  defp reaggregate_for(:first, rows), do: rows |> Enum.min_by(& &1.bucket) |> Map.get(:avg)

  defp reaggregate_for(:rate, rows) do
    sorted = Enum.sort_by(rows, & &1.bucket)
    pairs = Enum.chunk_every(sorted, 2, 1, :discard)

    {total_delta, total_dt} =
      Enum.reduce(pairs, {0.0, 0}, fn [a, b], {dv_acc, dt_acc} ->
        dt = b.bucket - a.bucket
        dv = b.last - a.last

        if dv >= 0 and dt > 0 do
          {dv_acc + dv, dt_acc + dt}
        else
          {dv_acc, dt_acc}
        end
      end)

    if total_dt > 0, do: total_delta / total_dt, else: 0.0
  end

  # Compute per-second rate from tier rows using the `last` value of each bucket.
  defp compute_tier_rate(rows, _resolution_seconds) when length(rows) < 2 do
    Enum.map(rows, fn {bucket, _value, _meta} -> {bucket, 0.0} end)
  end

  defp compute_tier_rate(rows, resolution_seconds) do
    rows
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map(fn [{bucket, _, prev_meta}, {_next_bucket, _, next_meta}] ->
      dv = next_meta.last - prev_meta.last

      if dv >= 0 do
        {bucket, dv / resolution_seconds}
      else
        {bucket, 0.0}
      end
    end)
  end

  defp latest_from_tier(store, tier_name, series_id) do
    builder = builder_for_series(store, series_id)

    {:ok, rows} =
      TimelessMetrics.SegmentBuilder.read_tier_latest(builder, tier_name, series_id)

    case rows do
      [[blob]] ->
        {_aggs, buckets} = TimelessMetrics.TierChunk.decode(blob)

        case buckets do
          [] ->
            {:ok, nil}

          _ ->
            latest = Enum.max_by(buckets, & &1.bucket)
            {:ok, {latest.bucket, latest.last}}
        end

      [] ->
        {:ok, nil}
    end
  end

  # --- Internals ---

  defp bucket_to_seconds(:minute), do: 60
  defp bucket_to_seconds(:hour), do: 3_600
  defp bucket_to_seconds(:day), do: 86_400
  defp bucket_to_seconds(:week), do: 604_800
  defp bucket_to_seconds({n, :seconds}), do: n
  defp bucket_to_seconds({n, :minutes}), do: n * 60
  defp bucket_to_seconds({n, :hours}), do: n * 3_600
  defp bucket_to_seconds({n, :days}), do: n * 86_400

  defp compute_aggregate(:avg, values, _points) do
    Enum.sum(values) / length(values)
  end

  defp compute_aggregate(:min, values, _points) do
    Enum.min(values)
  end

  defp compute_aggregate(:max, values, _points) do
    Enum.max(values)
  end

  defp compute_aggregate(:sum, values, _points) do
    Enum.sum(values)
  end

  defp compute_aggregate(:count, values, _points) do
    length(values)
  end

  defp compute_aggregate(:last, _values, points) do
    {_ts, val} = Enum.max_by(points, &elem(&1, 0))
    val
  end

  defp compute_aggregate(:first, _values, points) do
    {_ts, val} = Enum.min_by(points, &elem(&1, 0))
    val
  end

  defp compute_aggregate(:rate, _values, points) do
    sorted = Enum.sort_by(points, &elem(&1, 0))
    compute_rate(sorted)
  end

  # Rate: average per-second rate across adjacent points.
  # Skips negative deltas (counter resets).
  defp compute_rate(sorted) when length(sorted) < 2, do: 0.0

  defp compute_rate(sorted) do
    {total_delta, total_dt} =
      sorted
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.reduce({0.0, 0}, fn [{t1, v1}, {t2, v2}], {delta_acc, dt_acc} ->
        dt = t2 - t1
        dv = v2 - v1

        if dv >= 0 and dt > 0 do
          {delta_acc + dv, dt_acc + dt}
        else
          # Counter reset or same timestamp — skip
          {delta_acc, dt_acc}
        end
      end)

    if total_dt > 0, do: total_delta / total_dt, else: 0.0
  end
end
