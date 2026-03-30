defmodule TimelessMetrics.RustEngine do
  @moduledoc """
  Rust-native storage engine backend for TimelessMetrics.

  Handles the hot path for raw point writes and queries through a single Rust NIF
  resource. The Elixir application still owns supervision, HTTP routing, alerts,
  annotations, scrape targets, and rollup metadata.

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

  def write_resolved(store, series_id, value, timestamp) do
    case Nif.engine_write_batch_raw(ref(store), encode_raw_batch([{series_id, timestamp, value}])) do
      {:ok, :ok} -> :ok
      {:error, _} = error -> error
    end
  end

  def resolve_series(store, metric_name, labels) do
    cache = cache_ref(store)
    key = {metric_name, labels}

    case :ets.lookup(cache, key) do
      [{^key, series_id}] ->
        {:ok, series_id}

      [] ->
        with {:ok, series_id} <-
               Nif.engine_resolve_series(ref(store), metric_name, labels)
               |> unwrap_nif_ok() do
          cache_series_id(cache, key, series_id)
          {:ok, series_id}
        end
    end
  end

  def resolve_series_batch(store, pairs) do
    if pairs == [] do
      {:ok, %{}}
    else
      cache = cache_ref(store)

      {resolved, missing} =
        Enum.reduce(pairs, {%{}, %{}}, fn {metric, labels}, {resolved, missing} ->
          key = {metric, labels}

          cond do
            Map.has_key?(resolved, key) ->
              {resolved, missing}

            true ->
              case :ets.lookup(cache, key) do
                [{^key, series_id}] ->
                  {Map.put(resolved, key, series_id), missing}

                [] ->
                  {resolved, Map.put(missing, key, {metric, labels})}
              end
          end
        end)

      if map_size(missing) == 0 do
        {:ok, resolved}
      else
        missing_list = Map.values(missing)

        with {:ok, ids} <-
               Nif.engine_resolve_series_batch(ref(store), missing_list)
               |> unwrap_nif_ok() do
          merged =
            Enum.zip(missing_list, ids)
            |> Enum.reduce(resolved, fn {{metric, labels}, series_id}, acc ->
              key = {metric, labels}
              cache_series_id(cache, key, series_id)
              Map.put(acc, key, series_id)
            end)

          {:ok, merged}
        end
      end
    end
  end

  def write(store, metric_name, labels, value, timestamp) do
    with {:ok, series_id} <- resolve_series(store, metric_name, labels) do
      write_resolved(store, series_id, value, timestamp)
    end
  end

  def write_batch(store, entries) do
    if entries == [] do
      :ok
    else
      # Convert from timeless_metrics format {metric, labels, value} or {metric, labels, value, ts}
      now = System.os_time(:second)
      normalized = normalize_entries(entries, now)

      with {:ok, series_ids} <- resolve_series_ids(store, normalized) do
        raw_entries =
          Enum.map(normalized, fn {metric, labels, ts, value} ->
            {Map.fetch!(series_ids, {metric, labels}), ts, value}
          end)

        case Nif.engine_write_batch_raw(ref(store), encode_raw_batch(raw_entries)) do
          {:ok, :ok} -> :ok
          {:error, _} = error -> error
        end
      end
    end
  end

  def flush(store) do
    case Nif.engine_flush(ref(store)) do
      {:ok, :ok} -> :ok
      {:error, _} = error -> error
    end
  end

  def query_raw(store, metric_name, labels, opts) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))

    {:ok, results} =
      Nif.engine_query_range(ref(store), metric_name, labels, from, to)
      |> unwrap_nif_ok()

    case results do
      [{_labels, points}] ->
        {:ok, points}

      [] ->
        {:ok, []}

      multiple ->
        {:ok, Enum.flat_map(multiple, fn {_, pts} -> pts end) |> Enum.sort_by(&elem(&1, 0))}
    end
  end

  def query_multi(store, metric_name, label_filter, opts) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))

    {:ok, results} =
      Nif.engine_query_range(ref(store), metric_name, label_filter, from, to)
      |> unwrap_nif_ok()

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
      {:ok, results} =
        Nif.engine_query_aggregate(ref(store), metric_name, labels, from, to, agg)
        |> unwrap_nif_ok()

      case results do
        [{_labels, val}] -> {:ok, [{from, val}]}
        [] -> {:ok, []}
        _ -> {:ok, Enum.map(results, fn {_, val} -> {from, val} end)}
      end
    else
      # Bucketed — need to fetch raw points and bucket in Elixir
      # (the Rust engine doesn't have bucketed aggregation built in yet)
      {:ok, results} =
        Nif.engine_query_range(ref(store), metric_name, labels, from, to)
        |> unwrap_nif_ok()

      points =
        case results do
          [{_labels, pts}] ->
            pts

          [] ->
            []

          multiple ->
            Enum.flat_map(multiple, fn {_, pts} -> pts end) |> Enum.sort_by(&elem(&1, 0))
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

    if bucket_seconds do
      {:ok, results} =
        Nif.engine_query_range(ref(store), metric_name, label_filter, from, to)
        |> unwrap_nif_ok()

      formatted =
        results
        |> Enum.map(fn {labels, points} ->
          %{labels: labels, data: bucket_points(points, from, to, bucket_seconds, agg)}
        end)
        |> Enum.reject(fn %{data: d} -> d == [] end)

      {:ok, formatted}
    else
      {:ok, results} =
        Nif.engine_query_aggregate(ref(store), metric_name, label_filter, from, to, agg)
        |> unwrap_nif_ok()

      formatted =
        results
        |> Enum.map(fn {labels, value} ->
          %{labels: labels, data: [{from, value}]}
        end)
        |> Enum.reject(fn %{data: d} -> d == [] end)

      {:ok, formatted}
    end
  end

  def latest(store, metric_name, labels) do
    now = System.os_time(:second)

    {:ok, results} =
      Nif.engine_query_range(ref(store), metric_name, labels, now - 300, now)
      |> unwrap_nif_ok()

    case results do
      [{_labels, points}] when points != [] ->
        {:ok, List.last(points)}

      _ ->
        {:ok, nil}
    end
  end

  def list_metrics(store) do
    Nif.engine_list_metrics(ref(store))
    |> unwrap_nif_ok()
  end

  def list_series(store, metric_name) do
    {:ok, series} =
      Nif.engine_list_series(ref(store), metric_name)
      |> unwrap_nif_ok()

    {:ok, Enum.map(series, fn labels -> %{labels: labels} end)}
  end

  def label_values(store, metric_name, label_key) do
    Nif.engine_label_values(ref(store), metric_name, label_key)
    |> unwrap_nif_ok()
  end

  def find_series(store, metric_name, label_filter) do
    {:ok, series} =
      Nif.engine_list_series(ref(store), metric_name)
      |> unwrap_nif_ok()

    filter_series(series, label_filter)
  end

  def delete_before(store, before_ts) do
    Nif.engine_delete_before(ref(store), before_ts)
  end

  def info(store) do
    {:ok, raw} =
      Nif.engine_info(ref(store))
      |> unwrap_nif_ok()

    data_dir = :persistent_term.get({TimelessMetrics, store, :data_dir}, nil)
    total_points = trunc(raw["total_points"])
    storage_bytes = trunc(raw["total_bytes"])
    disk_points = trunc(raw["disk_points"])

    %{
      series_count: trunc(raw["series_count"]),
      disk_points: disk_points,
      total_points: total_points,
      points_ingested: total_points,
      storage_bytes: storage_bytes,
      compressed_bytes: storage_bytes,
      bytes_per_point: raw["bytes_per_point"],
      raw_buffer_points: trunc(raw["buffered_points"]),
      buffer_points: trunc(raw["buffered_points"]),
      block_count: trunc(raw["chunk_count"]),
      process_count: 1,
      index_ets_bytes: 0,
      buffer_memory_bytes: trunc(raw["buffer_memory_bytes"] || 0),
      daily_rollup_rows: 0,
      db_path: if(data_dir, do: Path.join(data_dir, "metrics.db"), else: nil),
      oldest_timestamp:
        case raw["oldest_timestamp"] do
          nil -> nil
          ts -> trunc(ts)
        end,
      newest_timestamp:
        case raw["newest_timestamp"] do
          nil -> nil
          ts -> trunc(ts)
        end
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

    engine =
      Nif.engine_new(
        engine_dir,
        @flush_threshold,
        @min_flush_size,
        @compression_level,
        @memory_budget_mb
      )

    cache =
      :ets.new(__MODULE__, [
        :set,
        :public,
        {:read_concurrency, true},
        {:write_concurrency, :auto},
        {:decentralized_counters, true}
      ])

    :persistent_term.put({__MODULE__, store}, engine)
    :persistent_term.put({__MODULE__, store, :series_cache}, cache)

    Process.flag(:trap_exit, true)
    schedule_flush()
    schedule_cold_flush()

    {:ok, %{store: store, engine: engine, cache: cache}}
  end

  @impl true
  def handle_info(:periodic_flush, state) do
    _ = Nif.engine_flush_pending(state.engine)
    schedule_flush()
    {:noreply, state}
  end

  @impl true
  def handle_info(:cold_flush, state) do
    _ = Nif.engine_flush_cold(state.engine, 300)
    schedule_cold_flush()
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    _ = Nif.engine_shutdown(state.engine)
    :persistent_term.erase({__MODULE__, state.store})
    :persistent_term.erase({__MODULE__, state.store, :series_cache})
    _ = :ets.delete(state.cache)
    :ok
  end

  defp schedule_flush, do: Process.send_after(self(), :periodic_flush, @flush_interval)
  defp schedule_cold_flush, do: Process.send_after(self(), :cold_flush, @cold_flush_interval)

  # ── Helpers ─────────────────────────────────────────────────────────

  defp unwrap_nif_ok({:ok, {:ok, value}}), do: {:ok, value}
  defp unwrap_nif_ok({:ok, value}), do: {:ok, value}
  defp unwrap_nif_ok({:error, _} = error), do: error

  defp cache_ref(store) do
    :persistent_term.get({__MODULE__, store, :series_cache})
  end

  defp cache_series_id(cache, key, series_id) do
    true = :ets.insert(cache, {key, series_id})
    series_id
  end

  defp normalize_entries(entries, now) do
    Enum.map(entries, fn
      {metric, labels, value} -> {metric, labels, now, value}
      {metric, labels, value, ts} -> {metric, labels, ts, value}
    end)
  end

  defp resolve_series_ids(store, entries) do
    pairs =
      entries
      |> Enum.map(fn {metric, labels, _ts, _value} -> {metric, labels} end)

    resolve_series_batch(store, pairs)
  end

  defp encode_raw_batch(entries) do
    for {series_id, ts, value} <- entries, into: <<>> do
      <<series_id::signed-native-64, ts::signed-native-64, value * 1.0::float-native-64>>
    end
  end

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
        nil ->
          []

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
