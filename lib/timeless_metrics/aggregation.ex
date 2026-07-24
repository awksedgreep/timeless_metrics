defmodule TimelessMetrics.Aggregation do
  @moduledoc """
  Shared aggregation helpers for the actor engine.

  Extracted from `TimelessMetrics.Query` — bucket_to_seconds, compute_aggregate,
  and bucket_points are needed by both the sharded query engine and the actor
  engine's in-process aggregation.
  """

  @doc "Convert a bucket spec to seconds."
  def bucket_to_seconds(:minute), do: 60
  def bucket_to_seconds(:hour), do: 3_600
  def bucket_to_seconds(:day), do: 86_400
  def bucket_to_seconds(:week), do: 604_800
  def bucket_to_seconds({n, :seconds}), do: n
  def bucket_to_seconds({n, :minutes}), do: n * 60
  def bucket_to_seconds({n, :hours}), do: n * 3_600
  def bucket_to_seconds({n, :days}), do: n * 86_400
  def bucket_to_seconds(n) when is_integer(n), do: n

  @doc """
  Group points into time buckets and compute an aggregate per bucket.

  Returns `[{bucket_timestamp, aggregate_value}, ...]` sorted by timestamp.
  """
  def bucket_points(points, bucket_seconds, agg_fn) do
    points
    |> Enum.group_by(fn {ts, _val} -> div(ts, bucket_seconds) * bucket_seconds end)
    |> Enum.map(fn {bucket, bucket_points} ->
      values = Enum.map(bucket_points, &elem(&1, 1))
      {bucket, compute_aggregate(agg_fn, values, bucket_points)}
    end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  @doc """
  Per-bucket counter rate with carry-in.

  Takes timestamp-sorted points and an alignment function `ts -> bucket_ts`.
  Each bucket's rate is computed from pairwise deltas over the bucket's points
  plus the last point of the previous bucket (so one-sample-per-bucket data —
  the common scrape case — still yields rates). Negative deltas (counter
  resets) are skipped. Buckets with no computable delta are omitted, matching
  Prometheus/VM (no rate at the first sample).

  Returns `[{bucket_timestamp, rate_per_second}, ...]` sorted by timestamp.
  """
  def bucket_rate(sorted_points, align_fun) do
    sorted_points
    |> Enum.chunk_by(fn {ts, _} -> align_fun.(ts) end)
    |> Enum.map_reduce(nil, fn bucket_pts, carry ->
      {ts0, _} = hd(bucket_pts)
      bucket = align_fun.(ts0)
      seq = if carry, do: [carry | bucket_pts], else: bucket_pts

      {total_dv, total_dt} =
        seq
        |> Enum.chunk_every(2, 1, :discard)
        |> Enum.reduce({0.0, 0}, fn [{t1, v1}, {t2, v2}], {dv_acc, dt_acc} ->
          if v2 >= v1 and t2 > t1 do
            {dv_acc + (v2 - v1), dt_acc + (t2 - t1)}
          else
            {dv_acc, dt_acc}
          end
        end)

      rate = if total_dt > 0, do: {bucket, total_dv / total_dt}, else: nil
      {rate, List.last(bucket_pts)}
    end)
    |> elem(0)
    |> Enum.reject(&is_nil/1)
  end

  @doc "Compute an aggregate value from a list of values."
  def compute_aggregate(:avg, values, _points) do
    Enum.sum(values) / length(values)
  end

  def compute_aggregate(:min, values, _points) do
    Enum.min(values)
  end

  def compute_aggregate(:max, values, _points) do
    Enum.max(values)
  end

  def compute_aggregate(:sum, values, _points) do
    Enum.sum(values)
  end

  def compute_aggregate(:count, values, _points) do
    length(values)
  end

  def compute_aggregate(:last, _values, points) do
    {_ts, val} = Enum.max_by(points, &elem(&1, 0))
    val
  end

  def compute_aggregate(:first, _values, points) do
    {_ts, val} = Enum.min_by(points, &elem(&1, 0))
    val
  end

  def compute_aggregate(:rate, _values, points) do
    sorted = Enum.sort_by(points, &elem(&1, 0))
    compute_rate(sorted)
  end

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
          {delta_acc, dt_acc}
        end
      end)

    if total_dt > 0, do: total_delta / total_dt, else: 0.0
  end
end
