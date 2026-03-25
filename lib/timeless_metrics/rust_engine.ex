defmodule TimelessMetrics.RustEngine do
  @moduledoc """
  Rust-native storage engine backend for TimelessMetrics.

  Replaces ETS shards + SeriesRegistry + Buffer + SegmentBuilder + SQLite
  with a single Rust NIF resource (DashMap + Pco + BTreeMap).

  Started as a child of the store's supervisor. The engine reference is
  stored in persistent_term for zero-cost access on the hot path.
  """

  use GenServer

  alias TimelessMetrics.RustEngine.Nif

  @flush_threshold 8_192
  @min_flush_size 64
  @compression_level 8
  @memory_budget_mb 0
  @flush_interval :timer.seconds(10)
  @cold_flush_interval :timer.minutes(5)

  # ── Public API (called by TimelessMetrics module) ───────────────────

  def ref(store) do
    :persistent_term.get({__MODULE__, store})
  end

  def write(store, metric_name, labels, value, timestamp) do
    Nif.engine_write_batch_labeled(ref(store), [{metric_name, labels, timestamp, value}])
  end

  def write_batch(store, entries) do
    # Convert from timeless_metrics format {metric, labels, value} or {metric, labels, value, ts}
    now = System.os_time(:second)

    labeled =
      Enum.map(entries, fn
        {metric, labels, value} -> {metric, labels, now, value}
        {metric, labels, value, ts} -> {metric, labels, ts, value}
      end)

    Nif.engine_write_batch_labeled(ref(store), labeled)
  end

  def flush(store) do
    Nif.engine_flush(ref(store))
  end

  def query_raw(store, metric_name, labels, opts) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))

    {:ok, results} = Nif.engine_query_range(ref(store), metric_name, labels, from, to)

    case results do
      [{_labels, points}] -> {:ok, points}
      [] -> {:ok, []}
      multiple -> {:ok, Enum.flat_map(multiple, fn {_, pts} -> pts end) |> Enum.sort_by(&elem(&1, 0))}
    end
  end

  def query_multi(store, metric_name, label_filter, opts) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))

    {:ok, results} = Nif.engine_query_range(ref(store), metric_name, label_filter, from, to)

    formatted =
      results
      |> Enum.map(fn {labels, points} -> %{labels: labels, points: points} end)
      |> Enum.reject(fn %{points: pts} -> pts == [] end)

    {:ok, formatted}
  end

  def query_aggregate(store, metric_name, labels, opts) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))
    bucket = opts[:bucket]
    agg = Keyword.get(opts, :aggregate, :avg)

    bucket_seconds = bucket_to_seconds(bucket)

    if bucket_seconds == nil do
      # No bucketing — return scalar aggregate
      {:ok, results} = Nif.engine_query_aggregate(ref(store), metric_name, labels, from, to, agg)
      case results do
        [{_labels, val}] -> {:ok, [{from, val}]}
        [] -> {:ok, []}
        _ -> {:ok, Enum.map(results, fn {_, val} -> {from, val} end)}
      end
    else
      # Bucketed — need to fetch raw points and bucket in Elixir
      # (the Rust engine doesn't have bucketed aggregation built in yet)
      {:ok, results} = Nif.engine_query_range(ref(store), metric_name, labels, from, to)

      points = case results do
        [{_labels, pts}] -> pts
        [] -> []
        multiple -> Enum.flat_map(multiple, fn {_, pts} -> pts end) |> Enum.sort_by(&elem(&1, 0))
      end

      bucketed = bucket_points(points, from, to, bucket_seconds, agg)
      {:ok, bucketed}
    end
  end

  def query_aggregate_multi(store, metric_name, label_filter, opts) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))
    bucket = opts[:bucket]
    agg = Keyword.get(opts, :aggregate, :avg)

    bucket_seconds = bucket_to_seconds(bucket)

    {:ok, results} = Nif.engine_query_range(ref(store), metric_name, label_filter, from, to)

    formatted =
      results
      |> Enum.map(fn {labels, points} ->
        data = if bucket_seconds do
          bucket_points(points, from, to, bucket_seconds, agg)
        else
          points
        end
        %{labels: labels, data: data}
      end)
      |> Enum.reject(fn %{data: d} -> d == [] end)

    {:ok, formatted}
  end

  def latest(store, metric_name, labels) do
    now = System.os_time(:second)
    {:ok, results} = Nif.engine_query_range(ref(store), metric_name, labels, now - 300, now)

    case results do
      [{_labels, points}] when points != [] ->
        {:ok, List.last(points)}
      _ ->
        {:ok, nil}
    end
  end

  def list_metrics(store) do
    Nif.engine_list_metrics(ref(store))
  end

  def list_series(store, metric_name) do
    {:ok, series} = Nif.engine_list_series(ref(store), metric_name)
    {:ok, Enum.map(series, fn labels -> %{labels: labels} end)}
  end

  def label_values(store, metric_name, label_key) do
    Nif.engine_label_values(ref(store), metric_name, label_key)
  end

  def find_series(store, metric_name, label_filter) do
    {:ok, results} = Nif.engine_query_range(ref(store), metric_name, label_filter, 0, 0)
    # Just need the labels, not the points
    {:ok, series} = Nif.engine_list_series(ref(store), metric_name)
    filter_series(series, label_filter)
  end

  def delete_before(store, before_ts) do
    Nif.engine_delete_before(ref(store), before_ts)
  end

  def info(store) do
    raw = Nif.engine_info(ref(store))

    %{
      series_count: trunc(raw["series_count"]),
      total_points: trunc(raw["total_points"]),
      storage_bytes: trunc(raw["total_bytes"]),
      bytes_per_point: raw["bytes_per_point"],
      raw_buffer_points: trunc(raw["buffered_points"]),
      block_count: trunc(raw["chunk_count"]),
      process_count: 1,
      index_ets_bytes: 0,
      daily_rollup_rows: 0,
      db_path: nil
    }
  end

  # ── GenServer ───────────────────────────────────────────────────────

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :"#{opts[:store]}_rust_engine")
  end

  @impl true
  def init(opts) do
    store = Keyword.fetch!(opts, :store)
    data_dir = Keyword.get(opts, :data_dir, "data")

    engine_dir = Path.join(data_dir, "rust_engine")
    File.mkdir_p!(engine_dir)

    engine = Nif.engine_new(engine_dir, @flush_threshold, @min_flush_size, @compression_level, @memory_budget_mb)
    :persistent_term.put({__MODULE__, store}, engine)

    Process.flag(:trap_exit, true)
    schedule_flush()
    schedule_cold_flush()

    {:ok, %{store: store, engine: engine}}
  end

  @impl true
  def handle_info(:periodic_flush, state) do
    Nif.engine_flush_pending(state.engine)
    schedule_flush()
    {:noreply, state}
  end

  @impl true
  def handle_info(:cold_flush, state) do
    Nif.engine_flush_cold(state.engine, 300)
    schedule_cold_flush()
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    Nif.engine_shutdown(state.engine)
    :ok
  end

  defp schedule_flush, do: Process.send_after(self(), :periodic_flush, @flush_interval)
  defp schedule_cold_flush, do: Process.send_after(self(), :cold_flush, @cold_flush_interval)

  # ── Helpers ─────────────────────────────────────────────────────────

  defp bucket_to_seconds(nil), do: nil
  defp bucket_to_seconds(:minute), do: 60
  defp bucket_to_seconds(:hour), do: 3600
  defp bucket_to_seconds(:day), do: 86400
  defp bucket_to_seconds({n, :seconds}), do: n
  defp bucket_to_seconds({n, :minutes}), do: n * 60
  defp bucket_to_seconds({n, :hours}), do: n * 3600
  defp bucket_to_seconds(_), do: 60

  defp bucket_points([], _from, _to, _step, _agg), do: []
  defp bucket_points(points, from, to, step, agg) do
    buckets = Stream.iterate(from, &(&1 + step)) |> Enum.take_while(&(&1 < to))
    point_map = Enum.group_by(points, fn {ts, _} -> from + div(ts - from, step) * step end)

    Enum.flat_map(buckets, fn b ->
      case Map.get(point_map, b) do
        nil -> []
        pts ->
          vals = Enum.map(pts, &elem(&1, 1))
          [{b, aggregate_values(vals, agg)}]
      end
    end)
  end

  defp aggregate_values(vals, :avg), do: Enum.sum(vals) / length(vals)
  defp aggregate_values(vals, :min), do: Enum.min(vals)
  defp aggregate_values(vals, :max), do: Enum.max(vals)
  defp aggregate_values(vals, :sum), do: Enum.sum(vals)
  defp aggregate_values(vals, :count), do: length(vals) * 1.0
  defp aggregate_values([_ | _] = vals, :last), do: List.last(vals)
  defp aggregate_values([first | _], :first), do: first
  defp aggregate_values(vals, _), do: Enum.sum(vals) / length(vals)

  defp filter_series(series, filter) when map_size(filter) == 0, do: series
  defp filter_series(series, filter) do
    Enum.filter(series, fn labels ->
      Enum.all?(filter, fn {k, v} -> Map.get(labels, k) == v end)
    end)
  end
end
