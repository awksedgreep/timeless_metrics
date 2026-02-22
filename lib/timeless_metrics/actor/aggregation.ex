defmodule TimelessMetrics.Actor.Aggregation do
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
