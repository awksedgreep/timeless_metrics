defmodule TimelessMetrics do
  @moduledoc """
  Embedded time series storage for Elixir.

  TimelessMetrics runs as an embedded store inside your application and defaults to
  a Rust-backed engine for raw point ingestion and queries. Administrative data such
  as annotations, alerts, scrape targets, and rollup metadata stays on the Elixir side.

  ## Quick Start

      # Add to your supervision tree
      children = [
        {TimelessMetrics, name: :metrics, data_dir: "/tmp/metrics"}
      ]

      # Write metrics
      TimelessMetrics.write(:metrics, "cpu_usage", %{"host" => "web-1"}, 73.2)

      # Query
      TimelessMetrics.query(:metrics, "cpu_usage", %{"host" => "web-1"},
        from: System.os_time(:second) - 3600,
        to: System.os_time(:second)
      )
  """

  # Batch sizes above this threshold use parallel resolution + shard writes
  @parallel_batch_threshold 1_000

  defp rust_engine?(store) do
    :persistent_term.get({TimelessMetrics, store, :engine}, nil) == :rust
  end

  @doc "Start a TimelessMetrics instance as part of a supervision tree."
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {TimelessMetrics.Supervisor, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Write a single metric point.

  ## Parameters

    * `store` - The store name (atom)
    * `metric_name` - String metric name (e.g., "cpu_usage")
    * `labels` - Map of string labels (e.g., %{"host" => "web-1"})
    * `value` - Numeric value (float or integer)
    * `opts` - Optional keyword list:
      * `:timestamp` - Unix timestamp in seconds (default: now)
  """
  def write(store, metric_name, labels, value, opts \\ []) do
    timestamp = Keyword.get(opts, :timestamp, System.os_time(:second))
    TimelessMetrics.Stats.incr_writes(store)
    TimelessMetrics.Stats.add_points(store, 1)

    if rust_engine?(store) do
      TimelessMetrics.RustEngine.write(store, metric_name, labels, value, timestamp)
    else
      registry = :"#{store}_registry"
      series_id = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
      shard_count = buffer_shard_count(store)
      shard_idx = rem(abs(series_id), shard_count)
      TimelessMetrics.Buffer.write(:"#{store}_shard_#{shard_idx}", series_id, timestamp, value)
    end
  end

  @doc """
  Write a batch of metric points.

  Each entry is a tuple of `{metric_name, labels, value}` or
  `{metric_name, labels, value, timestamp}`.
  """
  def write_batch(store, entries) do
    TimelessMetrics.Stats.incr_writes(store)
    TimelessMetrics.Stats.add_points(store, length(entries))

    if rust_engine?(store) do
      TimelessMetrics.RustEngine.write_batch(store, entries)
    else
      write_batch_legacy(store, entries)
    end
  end

  defp write_batch_legacy(store, entries) do
    registry = :"#{store}_registry"
    shard_count = buffer_shard_count(store)

    if length(entries) >= @parallel_batch_threshold do
      chunk_size = max(div(length(entries), System.schedulers_online()), 1)

      entries
      |> Enum.chunk_every(chunk_size)
      |> Enum.map(fn chunk ->
        Task.async(fn ->
          chunk
          |> Enum.map(&resolve_and_normalize(registry, &1))
          |> Enum.group_by(fn {sid, _, _} -> rem(abs(sid), shard_count) end)
          |> Enum.each(fn {shard_idx, points} ->
            TimelessMetrics.Buffer.write_bulk(:"#{store}_shard_#{shard_idx}", points)
          end)
        end)
      end)
      |> Task.await_many()

      :ok
    else
      entries
      |> Enum.map(&resolve_and_normalize(registry, &1))
      |> group_and_write_shards(store, shard_count)
    end
  end

  @doc """
  Write entries directly, one per unique series. Same as write_batch/2
  for the sharded engine.
  """
  def write_each(store, entries) do
    write_batch(store, entries)
  end

  @doc """
  Resolve a series to an integer ID for use with `write_resolved/4`.

  Cache the result for repeated writes to the same series.
  """
  def resolve_series(store, metric_name, labels) do
    if rust_engine?(store) do
      TimelessMetrics.RustEngine.resolve_series(store, metric_name, labels)
    else
      registry = :"#{store}_registry"
      TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
    end
  end

  @doc """
  Write directly using a pre-resolved series ID. Zero lookup cost.

      sid = TimelessMetrics.resolve_series(:metrics, "cpu_usage", %{"host" => "web-1"})
      TimelessMetrics.write_resolved(:metrics, sid, 73.2, timestamp: ts)
  """
  def write_resolved(store, series_id, value, opts \\ []) do
    timestamp = Keyword.get(opts, :timestamp, System.os_time(:second))

    if rust_engine?(store) do
      TimelessMetrics.RustEngine.write_resolved(store, series_id, value, timestamp)
    else
      shard_count = buffer_shard_count(store)
      shard_idx = rem(abs(series_id), shard_count)
      TimelessMetrics.Buffer.write(:"#{store}_shard_#{shard_idx}", series_id, timestamp, value)
    end
  end

  @doc """
  Query raw time series points for a single series (exact label match).

  ## Options

    * `:from` - Start timestamp (unix seconds, default: 0)
    * `:to` - End timestamp (unix seconds, default: now)

  Returns `{:ok, [{timestamp, value}, ...]}`.
  """
  def query(store, metric_name, labels, opts \\ []) do
    TimelessMetrics.Stats.incr_queries(store)

    if rust_engine?(store) do
      TimelessMetrics.RustEngine.query_raw(store, metric_name, labels, opts)
    else
      registry = :"#{store}_registry"
      series_id = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
      TimelessMetrics.Query.raw(store, series_id, opts)
    end
  end

  @doc """
  Query raw points across multiple series matching a label filter.

  Returns `{:ok, [%{labels: %{...}, points: [{ts, val}, ...]}, ...]}`.
  """
  def query_multi(store, metric_name, label_filter \\ %{}, opts \\ []) do
    TimelessMetrics.Stats.incr_queries(store)

    if rust_engine?(store) do
      TimelessMetrics.RustEngine.query_multi(store, metric_name, label_filter, opts)
    else
      query_multi_legacy(store, metric_name, label_filter, opts)
    end
  end

  defp query_multi_legacy(store, metric_name, label_filter, opts) do
    matching = find_matching_series(store, metric_name, label_filter)

    results =
      matching
      |> Task.async_stream(
        fn {series_id, labels} ->
          {:ok, points} = TimelessMetrics.Query.raw(store, series_id, opts)
          %{labels: labels, points: points}
        end,
        max_concurrency: System.schedulers_online(),
        ordered: false
      )
      |> Enum.map(fn {:ok, result} -> result end)
      |> Enum.reject(fn %{points: pts} -> pts == [] end)

    {:ok, results}
  end

  @doc """
  Query with time-bucket aggregation for a single series (exact label match).

  ## Options

    * `:from` - Start timestamp
    * `:to` - End timestamp
    * `:bucket` - Bucket size: `:minute`, `:hour`, `:day`, or `{n, :seconds}`
    * `:aggregate` - Aggregate function: `:avg`, `:min`, `:max`, `:sum`, `:count`, `:last`, `:first`

  Returns `{:ok, [{bucket_timestamp, aggregate_value}, ...]}`.
  """
  def query_aggregate(store, metric_name, labels, opts) do
    if rust_engine?(store) do
      # Exact-label single series: filter via multi, then match the label set
      # exactly (the filter alone would also match series with extra labels).
      {:ok, results} = query_aggregate_multi(store, metric_name, labels, opts)

      case Enum.find(results, fn %{labels: l} -> l == labels end) do
        %{data: data} -> {:ok, data}
        nil -> {:ok, []}
      end
    else
      registry = :"#{store}_registry"
      schema = get_schema(store)
      series_id = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
      TimelessMetrics.Stats.incr_queries(store)
      TimelessMetrics.Query.aggregate(store, series_id, Keyword.put(opts, :schema, schema))
    end
  end

  @doc """
  Query with aggregation across multiple series matching a label filter.

  Returns `{:ok, [%{labels: %{...}, data: [{bucket_ts, agg_value}, ...]}, ...]}`.
  """
  def query_aggregate_multi(store, metric_name, label_filter \\ %{}, opts) do
    TimelessMetrics.Stats.incr_queries(store)

    if rust_engine?(store) do
      TimelessMetrics.RustEngine.query_aggregate_multi(store, metric_name, label_filter, opts)
    else
      query_aggregate_multi_legacy(store, metric_name, label_filter, opts)
    end
  end

  defp query_aggregate_multi_legacy(store, metric_name, label_filter, opts) do
    schema = get_schema(store)
    transform = Keyword.get(opts, :transform)
    matching = find_matching_series(store, metric_name, label_filter)
    shard_count = buffer_shard_count(store)
    query_opts = Keyword.put(opts, :schema, schema)

    by_shard =
      Enum.group_by(matching, fn {series_id, _labels} ->
        rem(abs(series_id), shard_count)
      end)

    results =
      by_shard
      |> Task.async_stream(
        fn {_shard_idx, shard_series} ->
          shard_series
          |> Task.async_stream(
            fn {series_id, labels} ->
              {:ok, buckets} = TimelessMetrics.Query.aggregate(store, series_id, query_opts)

              case TimelessMetrics.Transform.apply(buckets, transform) do
                [] -> nil
                data -> %{labels: labels, data: data}
              end
            end,
            max_concurrency: System.schedulers_online(),
            ordered: false
          )
          |> Enum.flat_map(fn
            {:ok, nil} -> []
            {:ok, result} -> [result]
          end)
        end,
        max_concurrency: shard_count,
        ordered: false
      )
      |> Enum.flat_map(fn {:ok, shard_results} -> shard_results end)

    {:ok, results}
  end

  @doc """
  Query with cross-series aggregation, grouping results by a label key.

  Returns `{:ok, [%{group: %{"hostname" => "host_0"}, data: [{ts, val}]}, ...]}`.
  """
  def query_aggregate_grouped(store, metric_name, label_filter \\ %{}, opts) do
    group_by = Keyword.fetch!(opts, :group_by)
    {:ok, results} = query_aggregate_multi(store, metric_name, label_filter, opts)

    aggregate_fn = cross_series_aggregate_fn(opts)

    grouped =
      results
      |> Enum.group_by(fn %{labels: l} -> Map.take(l, List.wrap(group_by)) end)
      |> Enum.map(fn {group, series_results} ->
        data = cross_aggregate(series_results, aggregate_fn)
        %{group: group, data: data}
      end)

    {:ok, grouped}
  end

  # Cross-series merge honors the explicit :cross_series_aggregate option and
  # falls back to the per-bucket :aggregate (historical behavior).
  defp cross_series_aggregate_fn(opts) do
    Keyword.get(opts, :cross_series_aggregate) || Keyword.get(opts, :aggregate, :avg)
  end

  @doc """
  Query with cross-series aggregation across multiple metrics, with group-by.

  Returns `{:ok, [%{group: %{...}, data: [{ts, val}]}, ...]}`.
  """
  def query_aggregate_grouped_metrics(store, metric_names, label_filter \\ %{}, opts)
      when is_list(metric_names) do
    all_results =
      metric_names
      |> Task.async_stream(fn metric ->
        {:ok, results} = query_aggregate_multi(store, metric, label_filter, opts)
        results
      end)
      |> Enum.flat_map(fn {:ok, results} -> results end)

    group_by = Keyword.fetch!(opts, :group_by)
    aggregate_fn = cross_series_aggregate_fn(opts)

    grouped =
      all_results
      |> Enum.group_by(fn %{labels: l} -> Map.take(l, List.wrap(group_by)) end)
      |> Enum.map(fn {group, series_results} ->
        data = cross_aggregate(series_results, aggregate_fn)
        %{group: group, data: data}
      end)

    {:ok, grouped}
  end

  @doc """
  Query with aggregation and threshold filtering.

  Returns `{:ok, [%{labels: %{...}, data: [{ts, val}]}, ...]}`.
  """
  def query_aggregate_multi_filtered(store, metric_name, label_filter \\ %{}, opts) do
    threshold = Keyword.get(opts, :threshold)
    threshold_fn = Keyword.get(opts, :threshold_fn, :last)
    {:ok, results} = query_aggregate_multi(store, metric_name, label_filter, opts)

    filtered =
      if threshold do
        Enum.filter(results, fn %{data: data} ->
          val =
            case threshold_fn do
              :last -> data |> List.last() |> elem(1)
              :max -> data |> Enum.map(&elem(&1, 1)) |> Enum.max(fn -> 0 end)
              :avg -> data |> Enum.map(&elem(&1, 1)) |> then(&(Enum.sum(&1) / max(length(&1), 1)))
            end

          compare_threshold(val, threshold)
        end)
      else
        results
      end

    {:ok, filtered}
  end

  @doc """
  Sort results by a value function and take top N.
  """
  def top_n(results, n, order_fn \\ &last_value/1) do
    results
    |> Enum.sort_by(order_fn, :desc)
    |> Enum.take(n)
  end

  defp compare_threshold(val, {:gt, t}), do: val > t
  defp compare_threshold(val, {:lt, t}), do: val < t
  defp compare_threshold(val, {:gte, t}), do: val >= t
  defp compare_threshold(val, {:lte, t}), do: val <= t
  defp compare_threshold(val, t) when is_number(t), do: val >= t

  defp last_value(%{data: []}), do: 0.0
  defp last_value(%{data: data}), do: data |> List.last() |> elem(1)

  @doc """
  Query with aggregation across multiple metric names matching a label filter.

  Returns `{:ok, [%{metric: name, labels: %{...}, data: [{ts, val}, ...]}, ...]}`.
  """
  def query_aggregate_multi_metrics(store, metric_names, label_filter \\ %{}, opts)
      when is_list(metric_names) do
    metric_names
    |> Task.async_stream(fn metric ->
      {:ok, results} = query_aggregate_multi(store, metric, label_filter, opts)
      Enum.map(results, &Map.put(&1, :metric, metric))
    end)
    |> Enum.flat_map(fn {:ok, results} -> results end)
    |> then(&{:ok, &1})
  end

  @doc """
  Query pre-computed daily rollup data.

  Returns `{:ok, [%{bucket: ts, avg: v, min: v, max: v, count: n, sum: v, last: v}, ...]}`.
  """
  def query_daily(store, metric_name, labels, from, to) do
    registry = :"#{store}_registry"
    series_id = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
    TimelessMetrics.Query.read_tier(store, :daily, series_id, from: from, to: to)
  end

  @doc """
  Get the latest value for a series.

  Returns `{:ok, {timestamp, value}}` or `{:ok, nil}`.
  """
  def latest(store, metric_name, labels) do
    if rust_engine?(store) do
      TimelessMetrics.RustEngine.latest(store, metric_name, labels)
    else
      registry = :"#{store}_registry"
      schema = get_schema(store)
      series_id = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
      TimelessMetrics.Query.latest(store, series_id, schema: schema)
    end
  end

  @doc """
  Get the latest value for ALL series matching a metric name and label filter.

  Returns `{:ok, [%{labels: %{...}, timestamp: ts, value: val}, ...]}`.
  """
  def latest_multi(store, metric_name, label_filter \\ %{}) do
    if rust_engine?(store) do
      TimelessMetrics.RustEngine.latest_multi(store, metric_name, label_filter)
    else
      latest_multi_legacy(store, metric_name, label_filter)
    end
  end

  defp latest_multi_legacy(store, metric_name, label_filter) do
    schema = get_schema(store)
    matching = find_matching_series(store, metric_name, label_filter)

    results =
      matching
      |> Task.async_stream(
        fn {series_id, labels} ->
          case TimelessMetrics.Query.latest(store, series_id, schema: schema) do
            {:ok, {ts, val}} -> %{labels: labels, timestamp: ts, value: val}
            {:ok, nil} -> nil
          end
        end,
        max_concurrency: System.schedulers_online(),
        ordered: false
      )
      |> Enum.flat_map(fn
        {:ok, nil} -> []
        {:ok, result} -> [result]
      end)

    {:ok, results}
  end

  # --- Text series API ---
  # Text series use the same write/query path — the sharded engine stores
  # values as-is in ETS and the SegmentBuilder handles codec selection.

  @doc "Write a single text metric point."
  def write_text(store, metric_name, labels, value, opts \\ []) do
    write(store, metric_name, labels, value, opts)
  end

  @doc "Write a batch of text metric points."
  def write_text_batch(store, entries) do
    write_batch(store, entries)
  end

  @doc "Query text time series points for a single series."
  def query_text(store, metric_name, labels, opts \\ []) do
    query(store, metric_name, labels, opts)
  end

  @doc "Query text points across multiple series matching a label filter."
  def query_text_multi(store, metric_name, label_filter \\ %{}, opts \\ []) do
    query_multi(store, metric_name, label_filter, opts)
  end

  @doc "Get the latest text value for a series."
  def latest_text(store, metric_name, labels) do
    latest(store, metric_name, labels)
  end

  @doc "No-op for sharded engine (no per-series blocks to merge)."
  def merge_now(_store), do: :noop

  @doc "Force flush all buffered data to disk."
  def flush(store) do
    if rust_engine?(store) do
      TimelessMetrics.RustEngine.flush(store)
    else
      flush_legacy(store)
    end
  end

  defp flush_legacy(store) do
    TimelessMetrics.SeriesRegistry.flush_pending(:"#{store}_registry")
    shard_count = buffer_shard_count(store)

    # Flush buffers → SegmentBuilder (sync)
    for i <- 0..(shard_count - 1) do
      GenServer.call(:"#{store}_shard_#{i}", :flush_sync, :infinity)
    end

    # Flush SegmentBuilder → disk (sync)
    for i <- 0..(shard_count - 1) do
      TimelessMetrics.SegmentBuilder.flush(:"#{store}_builder_#{i}")
    end

    :ok
  end

  @doc "Create a consistent online backup."
  def backup(store, target_dir) do
    if rust_engine?(store) do
      backup_rust(store, target_dir)
    else
      backup_legacy(store, target_dir)
    end
  end

  defp backup_rust(store, target_dir) do
    flush(store)
    data_dir = :persistent_term.get({TimelessMetrics, store, :data_dir})
    db = :"#{store}_db"

    File.mkdir_p!(target_dir)

    # SQLite backup (admin data: alerts, annotations, scrape targets)
    db_target = Path.join(target_dir, "metrics.db")
    TimelessMetrics.DB.write(db, "VACUUM INTO ?1", [db_target])

    # Copy Rust engine data
    engine_src = Path.join(data_dir, "rust_engine")
    engine_dst = Path.join(target_dir, "rust_engine")
    engine_bytes = copy_dir(engine_src, engine_dst)

    db_size =
      case File.stat(db_target) do
        {:ok, %{size: s}} -> s
        _ -> 0
      end

    {:ok,
     %{
       path: target_dir,
       files: ["metrics.db", "rust_engine"],
       total_bytes: db_size + engine_bytes
     }}
  end

  defp backup_legacy(store, target_dir) do
    # Flush pending series registrations to SQLite
    TimelessMetrics.SeriesRegistry.flush_pending(:"#{store}_registry")
    # Flush all buffers and segment builders to disk
    flush(store)

    data_dir = :persistent_term.get({TimelessMetrics, store, :data_dir})
    db = :"#{store}_db"

    File.mkdir_p!(target_dir)

    # 1. VACUUM INTO for SQLite
    db_target = Path.join(target_dir, "metrics.db")
    TimelessMetrics.DB.write(db, "VACUUM INTO ?1", [db_target])

    # 2. Copy shard directories
    shard_count = buffer_shard_count(store)

    shard_bytes =
      for i <- 0..(shard_count - 1) do
        src = Path.join(data_dir, "shard_#{i}")
        dst = Path.join(target_dir, "shard_#{i}")
        copy_dir(src, dst)
      end
      |> Enum.sum()

    db_size =
      case File.stat(db_target) do
        {:ok, %{size: s}} -> s
        _ -> 0
      end

    files =
      ["metrics.db"] ++
        for i <- 0..(shard_count - 1), do: "shard_#{i}"

    {:ok, %{path: target_dir, files: files, total_bytes: db_size + shard_bytes}}
  end

  defp copy_dir(src, dst) do
    case File.ls(src) do
      {:ok, entries} ->
        File.mkdir_p!(dst)

        Enum.reduce(entries, 0, fn entry, acc ->
          src_path = Path.join(src, entry)
          dst_path = Path.join(dst, entry)

          case File.stat(src_path) do
            {:ok, %{type: :directory}} ->
              acc + copy_dir(src_path, dst_path)

            {:ok, %{type: :regular}} ->
              File.cp!(src_path, dst_path)

              case File.stat(dst_path) do
                {:ok, %{size: s}} -> acc + s
                _ -> acc
              end

            _ ->
              acc
          end
        end)

      _ ->
        0
    end
  end

  @doc "Get store info and statistics."
  def info(store) do
    if rust_engine?(store) do
      TimelessMetrics.RustEngine.info(store)
    else
      info_legacy(store)
    end
  end

  defp info_legacy(store) do
    stats = TimelessMetrics.Stats.snapshot(store)
    registry = :"#{store}_registry"
    series_count = series_count(store, registry)
    data_dir = :persistent_term.get({TimelessMetrics, store, :data_dir}, nil)
    shard_stats = shard_stats(store)
    persisted_points = shard_stats.total_points
    points_ingested = max(stats.points_ingested, persisted_points)
    buffer_points = max(stats.points_ingested - persisted_points, 0)
    daily_rollup_rows = daily_rollup_rows(store)

    storage_bytes =
      case data_dir && File.ls(data_dir) do
        {:ok, entries} ->
          entries
          |> Enum.reduce(0, fn entry, acc ->
            path = Path.join(data_dir, entry)

            case File.stat(path) do
              {:ok, %{size: s, type: :regular}} -> acc + s
              _ -> acc + dir_file_bytes(path)
            end
          end)

        _ ->
          0
      end

    %{
      series_count: series_count,
      total_points: points_ingested,
      raw_buffer_points: buffer_points,
      storage_bytes: storage_bytes,
      points_ingested: points_ingested,
      queries: stats.queries,
      buffer_points: buffer_points,
      db_path: if(data_dir, do: Path.join(data_dir, "metrics.db"), else: nil),
      block_count: 0,
      bytes_per_point: if(points_ingested > 0, do: storage_bytes / points_ingested, else: 0.0),
      compressed_bytes: storage_bytes,
      daily_rollup_rows: daily_rollup_rows
    }
  end

  defp dir_file_bytes(dir) do
    case File.ls(dir) do
      {:ok, files} ->
        Enum.reduce(files, 0, fn f, acc ->
          path = Path.join(dir, f)

          case File.stat(path) do
            {:ok, %{size: size, type: :regular}} -> acc + size
            {:ok, %{type: :directory}} -> acc + dir_file_bytes(path)
            _ -> acc
          end
        end)

      _ ->
        0
    end
  end

  defp shard_stats(store) do
    0..(buffer_shard_count(store) - 1)
    |> Enum.map(fn shard_idx ->
      TimelessMetrics.SegmentBuilder.raw_stats(:"#{store}_builder_#{shard_idx}")
    end)
    |> Enum.reduce(
      %{segment_count: 0, total_points: 0, raw_bytes: 0, oldest_ts: nil, newest_ts: nil},
      fn stats, acc ->
        %{
          segment_count: acc.segment_count + stats.segment_count,
          total_points: acc.total_points + stats.total_points,
          raw_bytes: acc.raw_bytes + stats.raw_bytes,
          oldest_ts: min_ts(acc.oldest_ts, stats.oldest_ts),
          newest_ts: max_ts(acc.newest_ts, stats.newest_ts)
        }
      end
    )
  end

  defp daily_rollup_rows(store) do
    schema = get_schema(store)

    case Enum.find(schema.tiers, &(&1.name == :daily)) do
      nil ->
        0

      _daily_tier ->
        0..(buffer_shard_count(store) - 1)
        |> Enum.reduce(0, fn shard_idx, acc ->
          {_chunks, bucket_count, _compressed_bytes} =
            TimelessMetrics.SegmentBuilder.read_tier_stats(
              :"#{store}_builder_#{shard_idx}",
              :daily
            )

          acc + bucket_count
        end)
    end
  end

  defp series_count(store, registry) do
    max(TimelessMetrics.SeriesRegistry.count(registry), persisted_series_count(store))
  end

  defp persisted_series_count(store) do
    db = :"#{store}_db"

    case TimelessMetrics.DB.read(db, "SELECT COUNT(*) FROM series") do
      {:ok, [[count]]} -> count
      _ -> 0
    end
  end

  defp min_ts(nil, nil), do: nil
  defp min_ts(nil, ts), do: ts
  defp min_ts(ts, nil), do: ts
  defp min_ts(left, right), do: min(left, right)

  defp max_ts(nil, nil), do: nil
  defp max_ts(nil, ts), do: ts
  defp max_ts(ts, nil), do: ts
  defp max_ts(left, right), do: max(left, right)

  @doc "Force a daily rollup run."
  def rollup(store) do
    if rust_engine?(store) do
      # Rollup tiers are a legacy-engine feature; the Rust engine downsamples
      # via raw-first compaction instead.
      :ok
    else
      GenServer.call(:"#{store}_rollup", {:run, :all}, :infinity)
    end
  end

  @doc """
  Force retention enforcement now.
  """
  def enforce_retention(store) do
    if rust_engine?(store) do
      schema = get_schema(store)

      case schema.raw_retention_seconds do
        :forever ->
          :ok

        seconds when is_integer(seconds) ->
          cutoff = System.os_time(:second) - seconds
          _ = TimelessMetrics.RustEngine.delete_before(store, cutoff)
          :ok
      end
    else
      GenServer.call(:"#{store}_retention", :enforce, :infinity)
    end
  end

  @doc """
  List all distinct metric names in the store.

  Returns `{:ok, ["cpu_usage", "mem_usage", ...]}`.
  """
  def list_metrics(store) do
    if rust_engine?(store) do
      TimelessMetrics.RustEngine.list_metrics(store)
    else
      list_metrics_legacy(store)
    end
  end

  defp list_metrics_legacy(store) do
    TimelessMetrics.SeriesRegistry.flush_pending(:"#{store}_registry")
    db = :"#{store}_db"

    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
        """
        SELECT metric_name
        FROM series
        GROUP BY metric_name
        ORDER BY COUNT(*) DESC, metric_name ASC
        """
      )

    {:ok, Enum.map(rows, fn [name] -> name end)}
  end

  @doc """
  List all series for a given metric name.

  Returns `{:ok, [%{labels: %{"host" => "web-1"}, ...}, ...]}`.
  """
  def list_series(store, metric_name) do
    if rust_engine?(store) do
      TimelessMetrics.RustEngine.list_series(store, metric_name)
    else
      list_series_legacy(store, metric_name)
    end
  end

  defp list_series_legacy(store, metric_name) do
    TimelessMetrics.SeriesRegistry.flush_pending(:"#{store}_registry")
    db = :"#{store}_db"

    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
        "SELECT labels FROM series WHERE metric_name = ?1 ORDER BY labels",
        [metric_name]
      )

    {:ok, Enum.map(rows, fn [labels_str] -> %{labels: decode_labels(labels_str)} end)}
  end

  @doc """
  List distinct values for a specific label key across all series of a metric.

  Returns `{:ok, ["web-1", "web-2", ...]}`.
  """
  def label_values(store, metric_name, label_key) do
    if rust_engine?(store) do
      TimelessMetrics.RustEngine.label_values(store, metric_name, label_key)
    else
      label_values_legacy(store, metric_name, label_key)
    end
  end

  defp label_values_legacy(store, metric_name, label_key) do
    TimelessMetrics.SeriesRegistry.flush_pending(:"#{store}_registry")
    db = :"#{store}_db"

    {:ok, rows} =
      TimelessMetrics.DB.read(db, "SELECT labels FROM series WHERE metric_name = ?1", [
        metric_name
      ])

    result =
      rows
      |> Enum.map(fn [labels_str] -> decode_labels(labels_str) end)
      |> Enum.flat_map(fn labels -> Map.get(labels, label_key) |> List.wrap() end)
      |> Enum.uniq()
      |> Enum.sort()

    {:ok, result}
  end

  @doc """
  Register metadata for a metric (type, unit, description).
  """
  def register_metric(store, metric_name, metric_type, opts \\ []) do
    db = :"#{store}_db"
    type_str = to_string(metric_type)
    unit = Keyword.get(opts, :unit)
    description = Keyword.get(opts, :description)

    TimelessMetrics.DB.write(
      db,
      "INSERT OR REPLACE INTO metric_metadata (metric_name, metric_type, unit, description) VALUES (?1, ?2, ?3, ?4)",
      [metric_name, type_str, unit, description]
    )
  end

  @doc """
  Get metadata for a metric.

  Returns `{:ok, %{type: :gauge, unit: "%", description: "..."}}` or `{:ok, nil}`.
  """
  def get_metadata(store, metric_name) do
    db = :"#{store}_db"

    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
        "SELECT metric_type, unit, description FROM metric_metadata WHERE metric_name = ?1",
        [metric_name]
      )

    case rows do
      [[type, unit, desc]] ->
        {:ok, %{type: String.to_atom(type), unit: unit, description: desc}}

      [] ->
        {:ok, nil}
    end
  end

  @doc """
  Create an annotation (event marker).

  Returns `{:ok, id}`.
  """
  def annotate(store, timestamp, title, opts \\ []) do
    db = :"#{store}_db"
    description = Keyword.get(opts, :description)
    tags = Keyword.get(opts, :tags, []) |> Enum.join(",")
    created_at = System.os_time(:second)

    {:ok, id} =
      TimelessMetrics.DB.write_transaction(db, fn conn ->
        TimelessMetrics.DB.execute(
          conn,
          "INSERT INTO annotations (timestamp, title, description, tags, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
          [timestamp, title, description, tags, created_at]
        )

        {:ok, [[id]]} =
          TimelessMetrics.DB.execute(conn, "SELECT last_insert_rowid()", [])

        id
      end)

    {:ok, id}
  end

  @doc """
  Query annotations within a time range.

  Returns `{:ok, [%{id: n, timestamp: ts, title: "...", description: "...", tags: [...]}]}`.
  """
  def annotations(store, from, to, opts \\ []) do
    db = :"#{store}_db"
    tag_filter = Keyword.get(opts, :tags, [])

    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
        "SELECT id, timestamp, title, description, tags FROM annotations WHERE timestamp >= ?1 AND timestamp <= ?2 ORDER BY timestamp",
        [from, to]
      )

    results =
      rows
      |> Enum.map(fn [id, ts, title, desc, tags_str] ->
        tags =
          if tags_str && tags_str != "", do: String.split(tags_str, ",", trim: true), else: []

        %{id: id, timestamp: ts, title: title, description: desc, tags: tags}
      end)
      |> then(fn results ->
        if tag_filter == [] do
          results
        else
          filter_set = MapSet.new(tag_filter)

          Enum.filter(results, fn %{tags: tags} ->
            tags |> MapSet.new() |> MapSet.intersection(filter_set) |> MapSet.size() > 0
          end)
        end
      end)

    {:ok, results}
  end

  @doc "Delete an annotation by ID."
  def delete_annotation(store, id) do
    db = :"#{store}_db"
    TimelessMetrics.DB.write(db, "DELETE FROM annotations WHERE id = ?1", [id])
    :ok
  end

  @doc """
  Create an alert rule.

  Returns `{:ok, rule_id}`.
  """
  def create_alert(store, opts) do
    db = :"#{store}_db"
    TimelessMetrics.Alert.create_rule(db, opts)
  end

  @doc "List all alert rules with current state."
  def list_alerts(store) do
    db = :"#{store}_db"
    TimelessMetrics.Alert.list_rules(db)
  end

  @doc "Update an alert rule (partial update). Returns `:ok`."
  def update_alert(store, rule_id, opts) do
    db = :"#{store}_db"
    TimelessMetrics.Alert.update_rule(db, rule_id, opts)
  end

  @doc "Delete an alert rule."
  def delete_alert(store, rule_id) do
    db = :"#{store}_db"
    TimelessMetrics.Alert.delete_rule(db, rule_id)
  end

  @doc "Evaluate all alert rules against current data."
  def evaluate_alerts(store) do
    TimelessMetrics.Alert.evaluate(store)
  end

  @doc """
  List recent alert history entries.

  Options: `:limit`, `:rule_id`, `:acknowledged` (true/false/nil).
  """
  def alert_history(store, opts \\ []) do
    db = :"#{store}_db"
    TimelessMetrics.Alert.list_history(db, opts)
  end

  @doc "Acknowledge an alert history entry by ID."
  def acknowledge_alert(store, history_id) do
    db = :"#{store}_db"
    TimelessMetrics.Alert.acknowledge_alert(db, history_id)
  end

  @doc """
  Clear alert history entries.

  Options: `:acknowledged_only` (default true), `:before` (timestamp cutoff).
  """
  def clear_alert_history(store, opts \\ []) do
    db = :"#{store}_db"
    TimelessMetrics.Alert.clear_history(db, opts)
  end

  @doc """
  Forecast future values for matching series.

  Returns `{:ok, [%{labels: map, data: [{ts, val}], forecast: [{ts, val}]}, ...]}`.
  """
  def forecast(store, metric_name, labels, opts) do
    from = Keyword.fetch!(opts, :from)
    to = Keyword.get(opts, :to, System.os_time(:second))
    horizon = Keyword.fetch!(opts, :horizon)
    bucket = Keyword.get(opts, :bucket, {300, :seconds})
    aggregate = Keyword.get(opts, :aggregate, :avg)

    bucket_seconds = bucket_to_seconds(bucket)

    {:ok, results} =
      query_aggregate_multi(store, metric_name, labels,
        from: from,
        to: to,
        bucket: bucket,
        aggregate: aggregate
      )

    forecasts =
      Enum.map(results, fn %{labels: l, data: data} ->
        case TimelessMetrics.Forecast.predict(data, horizon: horizon, bucket: bucket_seconds) do
          {:ok, predictions} -> %{labels: l, data: data, forecast: predictions}
          {:error, _} -> %{labels: l, data: data, forecast: []}
        end
      end)

    {:ok, forecasts}
  end

  @doc """
  Detect anomalies in matching series.

  Returns `{:ok, [%{labels: map, analysis: [%{timestamp, value, expected, score, anomaly}]}, ...]}`.
  """
  def detect_anomalies(store, metric_name, labels, opts) do
    from = Keyword.fetch!(opts, :from)
    to = Keyword.get(opts, :to, System.os_time(:second))
    bucket = Keyword.get(opts, :bucket, {300, :seconds})
    aggregate = Keyword.get(opts, :aggregate, :avg)
    sensitivity = Keyword.get(opts, :sensitivity, :medium)

    {:ok, results} =
      query_aggregate_multi(store, metric_name, labels,
        from: from,
        to: to,
        bucket: bucket,
        aggregate: aggregate
      )

    detections =
      Enum.map(results, fn %{labels: l, data: data} ->
        case TimelessMetrics.Anomaly.detect(data, sensitivity: sensitivity) do
          {:ok, analysis} -> %{labels: l, analysis: analysis}
          {:error, _} -> %{labels: l, analysis: []}
        end
      end)

    {:ok, detections}
  end

  defp bucket_to_seconds(:minute), do: 60
  defp bucket_to_seconds(:hour), do: 3600
  defp bucket_to_seconds(:day), do: 86400
  defp bucket_to_seconds({n, :seconds}), do: n
  defp bucket_to_seconds(n) when is_integer(n), do: n

  @doc false
  def merge_series_data(series_data_list, aggregate_fn) do
    series_data_list
    |> Enum.flat_map(& &1)
    |> Enum.group_by(fn {ts, _val} -> ts end, fn {_ts, val} -> val end)
    |> Enum.sort_by(fn {ts, _vals} -> ts end)
    |> Enum.map(fn {ts, vals} ->
      {ts, apply_cross_aggregate(vals, aggregate_fn)}
    end)
  end

  defp apply_cross_aggregate(vals, :max), do: Enum.max(vals)
  defp apply_cross_aggregate(vals, :min), do: Enum.min(vals)
  defp apply_cross_aggregate(vals, :sum), do: Enum.sum(vals)
  defp apply_cross_aggregate(vals, :count), do: length(vals) / 1
  defp apply_cross_aggregate(vals, :avg), do: Enum.sum(vals) / length(vals)
  # Bucket-only aggregates (:last, :first, :rate) have no cross-series meaning
  defp apply_cross_aggregate(vals, _other), do: Enum.sum(vals) / length(vals)

  # --- Sharded engine helpers ---

  defp get_schema(store) do
    :persistent_term.get({TimelessMetrics, store, :schema})
  end

  defp find_matching_series(store, metric_name, label_filter) do
    registry = :"#{store}_registry"

    # Read from persistent_term (published) + ETS overflow (recent)
    # This is O(all_series) but avoids SQLite entirely on the hot path
    fwd_key = {TimelessMetrics.SeriesRegistry, registry, :forward}
    rev_key = {TimelessMetrics.SeriesRegistry, registry, :reverse}
    overflow = :"#{registry}_series_overflow"

    # Collect all series from persistent_term
    fwd_map = :persistent_term.get(fwd_key)
    _rev_map = :persistent_term.get(rev_key)

    # Series from persistent_term matching this metric
    published =
      fwd_map
      |> Enum.filter(fn {{m, _labels}, _id} -> m == metric_name end)
      |> Enum.map(fn {{_m, labels}, id} -> {id, labels} end)

    # Series from ETS overflow matching this metric
    overflow_entries =
      try do
        :ets.tab2list(overflow)
        |> Enum.flat_map(fn
          {{^metric_name, labels}, id} -> [{id, labels}]
          _ -> []
        end)
      rescue
        _ -> []
      end

    # Merge (overflow may duplicate published — dedup by ID)
    published_ids = MapSet.new(published, &elem(&1, 0))

    all_series =
      published ++
        Enum.reject(overflow_entries, fn {id, _} -> MapSet.member?(published_ids, id) end)

    # Apply label filter (shared matcher: exact, regex, and negative forms)
    compiled = TimelessMetrics.LabelMatch.compile(label_filter)

    Enum.filter(all_series, fn {_id, labels} ->
      TimelessMetrics.LabelMatch.match?(labels, compiled)
    end)
  end

  defp decode_labels(""), do: %{}

  defp decode_labels(labels_str) do
    labels_str
    |> String.split(",")
    |> Enum.map(fn pair ->
      case String.split(pair, "=", parts: 2) do
        [k, v] -> {k, v}
        [k] -> {k, ""}
      end
    end)
    |> Map.new()
  end

  defp cross_aggregate(series_results, aggregate_fn) do
    series_data_list = Enum.map(series_results, fn %{data: data} -> data end)

    series_data_list
    |> Enum.flat_map(& &1)
    |> Enum.group_by(fn {ts, _val} -> ts end, fn {_ts, val} -> val end)
    |> Enum.sort_by(fn {ts, _vals} -> ts end)
    |> Enum.map(fn {ts, vals} ->
      {ts, apply_cross_aggregate(vals, aggregate_fn)}
    end)
  end

  defp buffer_shard_count(store) do
    :persistent_term.get({TimelessMetrics, store, :shard_count})
  end

  defp resolve_and_normalize(registry, {metric_name, labels, value}) do
    sid = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
    {sid, System.os_time(:second), value}
  end

  defp resolve_and_normalize(registry, {metric_name, labels, value, ts}) do
    sid = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
    {sid, ts, value}
  end

  defp group_and_write_shards(resolved_points, store, shard_count) do
    resolved_points
    |> Enum.group_by(fn {sid, _, _} -> rem(abs(sid), shard_count) end)
    |> Enum.each(fn {shard_idx, points} ->
      TimelessMetrics.Buffer.write_bulk(:"#{store}_shard_#{shard_idx}", points)
    end)

    :ok
  end
end
