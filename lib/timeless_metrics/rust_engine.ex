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
  @retention_interval :timer.hours(1)

  # ── Public API (called by TimelessMetrics module) ───────────────────

  def ref(store) do
    :persistent_term.get({__MODULE__, store})
  end

  def write_resolved(store, series_id, value, timestamp) do
    case Nif.engine_write_batch_raw(ref(store), encode_raw_batch([{series_id, timestamp, value}]))
         |> normalize_nif_result() do
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
               |> normalize_nif_result() do
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
               |> normalize_nif_result() do
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

        case Nif.engine_write_batch_raw(ref(store), encode_raw_batch(raw_entries))
             |> normalize_nif_result() do
          {:ok, :ok} -> :ok
          {:error, _} = error -> error
        end
      end
    end
  end

  @doc """
  Fused Prometheus ingest: parse exposition text and write all samples in a
  single NIF call — no per-sample terms cross the boundary. Samples without
  a timestamp get `default_ts` (epoch seconds).

  Returns `{:ok, count, errors}` where count is samples written and errors
  is malformed line count. Note: labels are stored exactly as scraped; no
  relabeling is applied. Callers needing relabel rules must use
  `PrometheusNif.parse/1` + `write_batch/2` instead.
  """
  def ingest_prometheus(store, body, default_ts \\ nil) do
    ts = default_ts || System.os_time(:second)

    case Nif.engine_ingest_prometheus(ref(store), body, ts) |> normalize_nif_result() do
      {:ok, {count, errors}} -> {:ok, count, errors}
      {:error, _} = error -> error
    end
  end

  def flush(store) do
    case Nif.engine_flush(ref(store))
         |> normalize_nif_result() do
      {:ok, :ok} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Force a compaction pass: merge raw and undersized chunks older than
  `cutoff_ts` into large pco chunks (`:all` compacts regardless of age).
  Runs automatically on the cold-flush timer when the store uses
  `defer_compression: true`, sparing the last hour so narrow recent
  queries stay on small chunks. Returns `{:ok, series, chunks_replaced}`.
  """
  def compact(store, cutoff_ts \\ :all) do
    cutoff = if cutoff_ts == :all, do: 9_223_372_036_854_775_807, else: cutoff_ts

    case Nif.engine_compact(ref(store), cutoff) |> normalize_nif_result() do
      {:ok, {series, chunks}} -> {:ok, series, chunks}
      {:error, _} = error -> error
    end
  end

  def query_raw(store, metric_name, labels, opts) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))

    {:ok, results} =
      Nif.engine_query_range(ref(store), metric_name, labels, from, to)
      |> normalize_nif_result()

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

    {:ok, results} = query_range_filtered(store, metric_name, label_filter, from, to)

    formatted =
      results
      |> Enum.map(fn {labels, points} -> %{labels: labels, points: points} end)
      |> Enum.reject(fn %{points: pts} -> pts == [] end)

    {:ok, formatted}
  end

  @doc """
  Latest point per series matching a label filter, regardless of age.

  The NIF has no dedicated latest-point query yet, so this reads the full
  range and keeps the newest sample per series.
  """
  def latest_multi(store, metric_name, label_filter) do
    now = System.os_time(:second)
    {:ok, results} = query_range_filtered(store, metric_name, label_filter, 0, now)

    latest =
      Enum.flat_map(results, fn
        {_labels, []} ->
          []

        {labels, points} ->
          {ts, val} = Enum.max_by(points, &elem(&1, 0))
          [%{labels: labels, timestamp: ts, value: val}]
      end)

    {:ok, latest}
  end

  # The NIF label filter only supports exact string equality. Push the exact
  # matchers down and apply regex/negative/empty matchers on the returned
  # series labels.
  defp query_range_filtered(store, metric_name, label_filter, from, to) do
    {eq, complex} = TimelessMetrics.LabelMatch.split_pushdown(label_filter)

    {:ok, results} =
      Nif.engine_query_range(ref(store), metric_name, eq, from, to)
      |> normalize_nif_result()

    case complex do
      [] ->
        {:ok, results}

      _ ->
        compiled = TimelessMetrics.LabelMatch.compile(complex)

        {:ok,
         Enum.filter(results, fn {labels, _pts} ->
           TimelessMetrics.LabelMatch.match?(labels, compiled)
         end)}
    end
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
        |> normalize_nif_result()

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
        |> normalize_nif_result()

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
    transform = Keyword.get(opts, :transform)

    bucket_seconds = bucket_to_seconds(bucket)

    if bucket_seconds do
      {:ok, results} = query_range_filtered(store, metric_name, label_filter, from, to)

      formatted =
        results
        |> Enum.map(fn {labels, points} ->
          data =
            points
            |> bucket_points(from, to, bucket_seconds, agg)
            |> TimelessMetrics.Transform.apply(transform)

          %{labels: labels, data: data}
        end)
        |> Enum.reject(fn %{data: d} -> d == [] end)

      {:ok, formatted}
    else
      {eq, complex} = TimelessMetrics.LabelMatch.split_pushdown(label_filter)

      {:ok, results} =
        Nif.engine_query_aggregate(ref(store), metric_name, eq, from, to, agg)
        |> normalize_nif_result()

      results =
        case complex do
          [] ->
            results

          _ ->
            compiled = TimelessMetrics.LabelMatch.compile(complex)

            Enum.filter(results, fn {labels, _v} ->
              TimelessMetrics.LabelMatch.match?(labels, compiled)
            end)
        end

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
      Nif.engine_query_range(ref(store), metric_name, labels, 0, now)
      |> normalize_nif_result()

    case results do
      [{_labels, points}] when points != [] ->
        {:ok, Enum.max_by(points, &elem(&1, 0))}

      _ ->
        {:ok, nil}
    end
  end

  def list_metrics(store) do
    Nif.engine_list_metrics(ref(store))
    |> normalize_nif_result()
  end

  def list_series(store, metric_name) do
    {:ok, series} =
      Nif.engine_list_series(ref(store), metric_name)
      |> normalize_nif_result()

    {:ok, Enum.map(series, fn labels -> %{labels: labels} end)}
  end

  def label_values(store, metric_name, label_key) do
    Nif.engine_label_values(ref(store), metric_name, label_key)
    |> normalize_nif_result()
  end

  def find_series(store, metric_name, label_filter) do
    {:ok, series} =
      Nif.engine_list_series(ref(store), metric_name)
      |> normalize_nif_result()

    filter_series(series, label_filter)
  end

  def delete_before(store, before_ts) do
    Nif.engine_delete_before(ref(store), before_ts)
  end

  def info(store) do
    {:ok, raw} =
      Nif.engine_info(ref(store))
      |> normalize_nif_result()

    data_dir = :persistent_term.get({TimelessMetrics, store, :data_dir}, nil)
    total_points = raw_stat(raw, "total_points", 0) |> trunc()
    storage_bytes = raw_stat(raw, "total_bytes", 0) |> trunc()
    disk_points = raw_stat(raw, "disk_points", total_points) |> trunc()

    buffer_memory_bytes =
      raw_stat(raw, "buffer_memory_bytes", raw_stat(raw, "buffer_memory_mb", 0) * 1024 * 1024)
      |> trunc()

    %{
      series_count: raw_stat(raw, "series_count", 0) |> trunc(),
      disk_points: disk_points,
      total_points: total_points,
      points_ingested: total_points,
      storage_bytes: storage_bytes,
      compressed_bytes: storage_bytes,
      bytes_per_point: raw_stat(raw, "bytes_per_point", 0.0),
      raw_buffer_points: raw_stat(raw, "buffered_points", 0) |> trunc(),
      buffer_points: raw_stat(raw, "buffered_points", 0) |> trunc(),
      block_count: raw_stat(raw, "chunk_count", 0) |> trunc(),
      process_count: 1,
      index_ets_bytes: 0,
      buffer_memory_bytes: buffer_memory_bytes,
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
        @memory_budget_mb,
        Keyword.get(opts, :defer_compression, false)
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
    schedule_retention()

    {:ok, %{store: store, engine: engine, cache: cache}}
  end

  @impl true
  def handle_info(:periodic_flush, state) do
    _ = Nif.engine_flush_pending(state.engine)
    schedule_flush()
    {:noreply, state}
  end

  @impl true
  def handle_info(:retention, state) do
    # The Rust tree has no Retention process — enforce raw retention from the
    # schema here so Rust stores don't grow unbounded.
    case :persistent_term.get({TimelessMetrics, state.store, :schema}, nil) do
      %{raw_retention_seconds: seconds} when is_integer(seconds) ->
        cutoff = System.os_time(:second) - seconds
        _ = Nif.engine_delete_before(state.engine, cutoff)

      _ ->
        :ok
    end

    schedule_retention()
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
  defp schedule_retention, do: Process.send_after(self(), :retention, @retention_interval)

  # ── Helpers ─────────────────────────────────────────────────────────

  @doc false
  def normalize_nif_result({:ok, {:ok, value}}), do: {:ok, value}
  def normalize_nif_result({:ok, value}), do: {:ok, value}
  def normalize_nif_result({:error, _} = error), do: error
  def normalize_nif_result(value), do: {:ok, value}

  defp raw_stat(raw, key, default) do
    case Map.get(raw, key) do
      nil -> default
      value -> value
    end
  end

  defp cache_ref(store) do
    :persistent_term.get({__MODULE__, store, :series_cache})
  end

  defp cache_series_id(cache, key, series_id) do
    true = :ets.insert(cache, {copy_key(key), series_id})
    series_id
  end

  # Parsed metric/label binaries are sub-binaries of the scrape body
  # (see PrometheusNif); copying on the rare insert path keeps cache
  # entries from pinning entire request bodies in memory.
  defp copy_key({metric, labels}) do
    {:binary.copy(metric), Map.new(labels, fn {k, v} -> {:binary.copy(k), :binary.copy(v)} end)}
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

  # Buckets align to `from` and use the same per-bucket aggregate math as the
  # legacy engine (TimelessMetrics.Aggregation), so both engines return
  # identical results for identical data — including :rate (pairwise slopes
  # within each bucket) and points landing exactly on the range end.
  defp bucket_points([], _from, _to, _step, _agg), do: []

  defp bucket_points(points, from, _to, step, :rate) do
    points
    |> Enum.sort_by(&elem(&1, 0))
    |> TimelessMetrics.Aggregation.bucket_rate(fn ts -> from + div(ts - from, step) * step end)
  end

  defp bucket_points(points, from, _to, step, agg) do
    points
    |> Enum.group_by(fn {ts, _} -> from + div(ts - from, step) * step end)
    |> Enum.map(fn {bucket, pts} ->
      values = Enum.map(pts, &elem(&1, 1))
      {bucket, TimelessMetrics.Aggregation.compute_aggregate(agg, values, pts)}
    end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp filter_series(series, filter) when map_size(filter) == 0, do: series

  defp filter_series(series, filter) do
    compiled = TimelessMetrics.LabelMatch.compile(filter)
    Enum.filter(series, &TimelessMetrics.LabelMatch.match?(&1, compiled))
  end
end
