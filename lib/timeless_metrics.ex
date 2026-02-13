defmodule TimelessMetrics do
  @moduledoc """
  Embedded time series storage for Elixir.

  Combines Gorilla compression with SQLite for fast, compact metric storage
  with automatic rollups and configurable retention.

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
    * `labels` - Map of string labels (e.g., %{"host" => "web-1", "device_id" => "42"})
    * `value` - Numeric value (float or integer)
    * `opts` - Optional keyword list:
      * `:timestamp` - Unix timestamp in seconds (default: now)
  """
  def write(store, metric_name, labels, value, opts \\ []) do
    timestamp = Keyword.get(opts, :timestamp, System.os_time(:second))
    registry = :"#{store}_registry"
    series_id = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
    shard_count = buffer_shard_count(store)
    shard_idx = rem(abs(series_id), shard_count)
    shard_name = :"#{store}_shard_#{shard_idx}"
    TimelessMetrics.Buffer.write(shard_name, series_id, timestamp, value)
  end

  @doc """
  Resolve a series ID for a given metric name and labels.

  Call once per series (e.g., at poller startup) and cache the ID.
  Then use `write_resolved/4` on the hot path to skip the registry.

  Returns an integer series_id.
  """
  def resolve_series(store, metric_name, labels) do
    registry = :"#{store}_registry"
    TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
  end

  @doc """
  Write a metric point using a pre-resolved series ID.

  Bypasses the series registry entirely — zero per-write overhead.
  Use `resolve_series/3` to obtain the series_id first.

  ## Options

    * `:timestamp` - Unix timestamp in seconds (default: now)
  """
  def write_resolved(store, series_id, value, opts \\ []) do
    timestamp = Keyword.get(opts, :timestamp, System.os_time(:second))
    shard_count = buffer_shard_count(store)
    shard_idx = rem(abs(series_id), shard_count)
    shard_name = :"#{store}_shard_#{shard_idx}"
    TimelessMetrics.Buffer.write(shard_name, series_id, timestamp, value)
  end

  @doc """
  Write a batch of metric points using pre-resolved series IDs.

  Each entry is `{series_id, value}` or `{series_id, value, timestamp}`.
  Bypasses the series registry entirely.
  """
  def write_batch_resolved(store, entries) do
    shard_count = buffer_shard_count(store)

    entries
    |> Enum.map(fn
      {sid, value} ->
        {sid, System.os_time(:second), value}

      {sid, value, ts} ->
        {sid, ts, value}
    end)
    |> group_and_write_shards(store, shard_count)
  end

  @doc """
  Write a batch of metric points.

  Each entry is a tuple of `{metric_name, labels, value}` or
  `{metric_name, labels, value, timestamp}`.
  """
  def write_batch(store, entries) do
    registry = :"#{store}_registry"
    shard_count = buffer_shard_count(store)

    if length(entries) >= @parallel_batch_threshold do
      # Parallel: each Task resolves its chunk AND writes to shards — no sync barrier
      chunk_size = max(div(length(entries), System.schedulers_online()), 1)

      entries
      |> Enum.chunk_every(chunk_size)
      |> Enum.map(fn chunk ->
        Task.async(fn ->
          chunk
          |> Enum.map(fn
            {metric_name, labels, value} ->
              sid = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
              {sid, System.os_time(:second), value}

            {metric_name, labels, value, ts} ->
              sid = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
              {sid, ts, value}
          end)
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
      |> Enum.map(fn
        {metric_name, labels, value} ->
          sid = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
          {sid, System.os_time(:second), value}

        {metric_name, labels, value, ts} ->
          sid = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
          {sid, ts, value}
      end)
      |> group_and_write_shards(store, shard_count)
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
    registry = :"#{store}_registry"
    series_id = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
    TimelessMetrics.Query.raw(store, series_id, opts)
  end

  @doc """
  Query raw points across multiple series matching a label filter.

  `label_filter` is a map of labels that must be present. An empty map
  matches all series for the given metric.

  Returns `{:ok, [%{labels: %{...}, points: [{ts, val}, ...]}, ...]}`.
  """
  def query_multi(store, metric_name, label_filter \\ %{}, opts \\ []) do
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
    registry = :"#{store}_registry"
    schema = get_schema(store)
    series_id = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
    TimelessMetrics.Query.aggregate(store, series_id, Keyword.put(opts, :schema, schema))
  end

  @doc """
  Query with aggregation across multiple series matching a label filter.

  Returns `{:ok, [%{labels: %{...}, data: [{bucket_ts, agg_value}, ...]}, ...]}`.
  """
  def query_aggregate_multi(store, metric_name, label_filter \\ %{}, opts) do
    schema = get_schema(store)
    transform = Keyword.get(opts, :transform)
    matching = find_matching_series(store, metric_name, label_filter)
    shard_count = buffer_shard_count(store)
    query_opts = Keyword.put(opts, :schema, schema)

    # Group series by shard so each task reads from a single shard's files,
    # eliminating cross-task contention on the same .seg / tier files.
    by_shard =
      Enum.group_by(matching, fn {series_id, _labels} ->
        rem(abs(series_id), shard_count)
      end)

    results =
      by_shard
      |> Task.async_stream(
        fn {_shard_idx, shard_series} ->
          Enum.flat_map(shard_series, fn {series_id, labels} ->
            {:ok, buckets} = TimelessMetrics.Query.aggregate(store, series_id, query_opts)

            case TimelessMetrics.Transform.apply(buckets, transform) do
              [] -> []
              data -> [%{labels: labels, data: data}]
            end
          end)
        end,
        max_concurrency: shard_count,
        ordered: false
      )
      |> Enum.flat_map(fn {:ok, shard_results} -> shard_results end)

    {:ok, results}
  end

  @doc """
  Read pre-computed rollup data directly from a tier.

  Returns `{:ok, [%{bucket: ts, avg: v, min: v, max: v, count: n, sum: v, last: v}, ...]}`.
  """
  def query_tier(store, tier_name, metric_name, labels, opts \\ []) do
    registry = :"#{store}_registry"
    series_id = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
    TimelessMetrics.Query.read_tier(store, tier_name, series_id, opts)
  end

  @doc """
  Get the latest value for a series.

  Returns `{:ok, {timestamp, value}}` or `{:ok, nil}`.
  """
  def latest(store, metric_name, labels) do
    registry = :"#{store}_registry"
    schema = get_schema(store)
    series_id = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
    TimelessMetrics.Query.latest(store, series_id, schema: schema)
  end

  @doc """
  Force flush all buffered data to disk.

  Useful for testing or before shutdown.
  """
  def flush(store) do
    shard_count = buffer_shard_count(store)

    # Flush all buffer shards synchronously (drains ETS → SegmentBuilder)
    for i <- 0..(shard_count - 1) do
      GenServer.call(:"#{store}_shard_#{i}", :flush_sync, :infinity)
    end

    # Flush all sharded segment builders in parallel
    tasks =
      for i <- 0..(shard_count - 1) do
        builder = :"#{store}_builder_#{i}"
        Task.async(fn -> TimelessMetrics.SegmentBuilder.flush(builder) end)
      end

    Task.await_many(tasks, :infinity)
  end

  @doc """
  Create a consistent online backup of all databases (main + shards).

  Flushes all in-flight data first, then uses SQLite's `VACUUM INTO` to
  create compacted snapshots while the service continues running.

  ## Parameters

    * `store` - The store name (atom)
    * `target_dir` - Directory to write backup files into (will be created)

  ## Returns

      {:ok, %{path: target_dir, files: [filenames], total_bytes: n}}

  ## Errors

      {:error, reason} if backup fails (e.g., target files already exist)
  """
  def backup(store, target_dir) do
    shard_count = buffer_shard_count(store)
    db = :"#{store}_db"

    # Flush all buffers and builders to capture in-flight data
    flush(store)

    File.mkdir_p!(target_dir)

    # Backup main DB
    main_target = Path.join(target_dir, "metrics.db")
    {:ok, _} = TimelessMetrics.DB.backup(db, main_target)

    # Backup all shard directories in parallel
    data_dir = Path.dirname(TimelessMetrics.DB.db_path(db))
    schema = get_schema(store)

    tasks =
      for i <- 0..(shard_count - 1) do
        Task.async(fn ->
          shard_src = Path.join(data_dir, "shard_#{i}")
          shard_dst = Path.join(target_dir, "shard_#{i}")

          # Copy raw segment files (.seg + .wal)
          raw_size = copy_dir_files(Path.join(shard_src, "raw"), Path.join(shard_dst, "raw"))

          # Copy tier chunk files (chunks.dat + index.ets)
          tier_size =
            Enum.reduce(schema.tiers, 0, fn tier, acc ->
              src = Path.join(shard_src, "tier_#{tier.name}")
              dst = Path.join(shard_dst, "tier_#{tier.name}")
              acc + copy_dir_files(src, dst)
            end)

          # Copy watermarks.bin
          File.mkdir_p!(shard_dst)

          wm_size =
            case File.cp(
                   Path.join(shard_src, "watermarks.bin"),
                   Path.join(shard_dst, "watermarks.bin")
                 ) do
              :ok ->
                case File.stat(Path.join(shard_dst, "watermarks.bin")) do
                  {:ok, %{size: s}} -> s
                  _ -> 0
                end

              _ ->
                0
            end

          {"shard_#{i}", raw_size + tier_size + wm_size}
        end)
      end

    shard_results = Task.await_many(tasks, :infinity)

    main_size = File.stat!(main_target).size
    files = ["metrics.db" | Enum.map(shard_results, &elem(&1, 0))]
    total_bytes = main_size + Enum.reduce(shard_results, 0, fn {_, size}, acc -> acc + size end)

    {:ok, %{path: target_dir, files: files, total_bytes: total_bytes}}
  end

  @doc """
  Get store info and statistics.

  Returns a map with raw segment stats, rollup tier stats, buffer sizes,
  watermark positions, and storage info.
  """
  def info(store) do
    db = :"#{store}_db"
    schema = get_schema(store)
    shard_count = buffer_shard_count(store)

    {:ok, [[series_count]]} = TimelessMetrics.DB.read(db, "SELECT COUNT(*) FROM series")

    # Aggregate raw segment stats from all shards
    {segment_count, total_points, raw_bytes, oldest_ts, newest_ts} =
      Enum.reduce(0..(shard_count - 1), {0, 0, 0, nil, nil}, fn i, {sc, tp, rb, oldest, newest} ->
        builder = :"#{store}_builder_#{i}"
        stats = TimelessMetrics.SegmentBuilder.raw_stats(builder)

        merged_oldest =
          cond do
            oldest == nil -> stats.oldest_ts
            stats.oldest_ts == nil -> oldest
            true -> min(oldest, stats.oldest_ts)
          end

        merged_newest =
          cond do
            newest == nil -> stats.newest_ts
            stats.newest_ts == nil -> newest
            true -> max(newest, stats.newest_ts)
          end

        {sc + stats.segment_count, tp + stats.total_points, rb + stats.raw_bytes, merged_oldest,
         merged_newest}
      end)

    # Sum all storage files (.db + shard dirs with .seg/.wal) for true on-disk usage
    db_path = TimelessMetrics.DB.db_path(db)
    data_dir = Path.dirname(db_path)

    storage_bytes =
      case File.ls(data_dir) do
        {:ok, files} ->
          db_bytes =
            files
            |> Enum.filter(&String.ends_with?(&1, ".db"))
            |> Enum.reduce(0, fn file, acc ->
              case File.stat(Path.join(data_dir, file)) do
                {:ok, %{size: size}} -> acc + size
                _ -> acc
              end
            end)

          shard_bytes =
            for i <- 0..(shard_count - 1), reduce: 0 do
              acc ->
                shard_dir = Path.join(data_dir, "shard_#{i}")

                # Raw segment files
                raw_dir = Path.join(shard_dir, "raw")
                raw_bytes = dir_file_bytes(raw_dir)

                # Tier chunk files
                tier_bytes =
                  Enum.reduce(schema.tiers, 0, fn tier, t_acc ->
                    tier_dir = Path.join(shard_dir, "tier_#{tier.name}")
                    t_acc + dir_file_bytes(tier_dir)
                  end)

                # Watermarks file
                wm_bytes =
                  case File.stat(Path.join(shard_dir, "watermarks.bin")) do
                    {:ok, %{size: s}} -> s
                    _ -> 0
                  end

                acc + raw_bytes + tier_bytes + wm_bytes
            end

          db_bytes + shard_bytes

        _ ->
          0
      end

    # Tier stats (aggregated from all shard ShardStores)
    tier_stats =
      Enum.map(schema.tiers, fn tier ->
        {chunks, buckets, compressed_bytes, dead_bytes, min_watermark} =
          Enum.reduce(0..(shard_count - 1), {0, 0, 0, 0, nil}, fn i,
                                                                  {ch_acc, bk_acc, cb_acc, db_acc,
                                                                   wm_acc} ->
            builder = :"#{store}_builder_#{i}"
            {c, b, cb} = TimelessMetrics.SegmentBuilder.read_tier_stats(builder, tier.name)
            {dead, _total} = TimelessMetrics.SegmentBuilder.tier_dead_bytes(builder, tier.name)

            wm = TimelessMetrics.SegmentBuilder.read_watermark(builder, tier.name)

            merged_wm =
              cond do
                wm_acc == nil -> wm
                wm == 0 -> 0
                true -> min(wm_acc, wm)
              end

            {ch_acc + c, bk_acc + b, cb_acc + cb, db_acc + dead, merged_wm}
          end)

        retention_label =
          if tier.retention_seconds == :forever,
            do: "forever",
            else: "#{div(tier.retention_seconds, 86_400)}d"

        {tier.name,
         %{
           rows: buckets,
           chunks: chunks,
           buckets: buckets,
           compressed_bytes: compressed_bytes,
           dead_bytes: dead_bytes,
           resolution_seconds: tier.resolution_seconds,
           retention: retention_label,
           watermark: min_watermark || 0
         }}
      end)
      |> Map.new()

    # Buffer sizes (ETS buffer + SegmentBuilder in-memory)
    shard_count = buffer_shard_count(store)

    {buffer_total, pending_total} =
      Enum.reduce(0..(shard_count - 1), {0, 0}, fn i, {buf_acc, pend_acc} ->
        buf = TimelessMetrics.Buffer.buffer_size(:"#{store}_shard_#{i}")
        pend = TimelessMetrics.SegmentBuilder.pending_point_count(:"#{store}_builder_#{i}")
        {buf_acc + buf, pend_acc + pend}
      end)

    # total_points from ShardStore.stats includes WAL entries (pending data
    # written during flush).  Don't add pending_total — after flush, pending
    # segments are in both WAL and in-memory, so adding would double-count.
    # buffer_total and pending_total remain as separate monitoring fields.
    all_points = total_points

    bytes_per_point =
      if total_points > 0, do: Float.round(raw_bytes / total_points, 2), else: 0.0

    %{
      series_count: series_count,
      segment_count: segment_count,
      total_points: all_points,
      segment_points: total_points,
      pending_points: pending_total,
      raw_compressed_bytes: raw_bytes,
      bytes_per_point: bytes_per_point,
      storage_bytes: storage_bytes,
      oldest_timestamp: oldest_ts,
      newest_timestamp: newest_ts,
      buffer_points: buffer_total,
      buffer_shards: shard_count,
      tiers: tier_stats,
      raw_retention: schema.raw_retention_seconds,
      db_path: db_path
    }
  end

  @doc """
  Force a rollup of all tiers (or a specific tier).
  """
  def rollup(store, tier \\ :all) do
    TimelessMetrics.Rollup.run(:"#{store}_rollup", tier)
  end

  @doc """
  Force a late-arrival catch-up scan.

  Re-processes a lookback window behind each tier's watermark to pick up
  data points that arrived after rollup had already advanced past their timestamps.
  """
  def catch_up(store) do
    TimelessMetrics.Rollup.catch_up(:"#{store}_rollup")
  end

  @doc """
  Compact tier chunk files across all shards to reclaim dead space.

  Dead space accumulates from read-modify-write during rollup (old versions
  of chunks remain in the append-only file). Compaction rewrites only live
  entries and atomically replaces the file.

  ## Options

    * `:threshold` - minimum dead/total ratio to trigger (default: 0.3)

  Returns `%{tier_name => reclaimed_bytes, ...}` for tiers that were compacted.
  """
  def compact(store, opts \\ []) do
    schema = get_schema(store)
    shard_count = buffer_shard_count(store)

    results =
      Enum.map(schema.tiers, fn tier ->
        reclaimed =
          for i <- 0..(shard_count - 1), reduce: 0 do
            acc ->
              builder = :"#{store}_builder_#{i}"

              case TimelessMetrics.SegmentBuilder.compact_tier(builder, tier.name, opts) do
                {:ok, bytes} -> acc + bytes
                :noop -> acc
              end
          end

        {tier.name, reclaimed}
      end)
      |> Enum.reject(fn {_name, bytes} -> bytes == 0 end)
      |> Map.new()

    results
  end

  @doc """
  Force retention enforcement now.
  """
  def enforce_retention(store) do
    TimelessMetrics.Retention.enforce(:"#{store}_retention")
  end

  @doc """
  List all distinct metric names in the store.

  Returns `{:ok, ["cpu_usage", "mem_usage", ...]}`.
  """
  def list_metrics(store) do
    db = :"#{store}_db"

    {:ok, rows} =
      TimelessMetrics.DB.read(db, "SELECT DISTINCT metric_name FROM series ORDER BY metric_name")

    {:ok, Enum.map(rows, fn [name] -> name end)}
  end

  @doc """
  List all series for a given metric name.

  Returns `{:ok, [%{labels: %{"host" => "web-1"}, ...}, ...]}`.
  """
  def list_series(store, metric_name) do
    db = :"#{store}_db"

    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
        "SELECT labels FROM series WHERE metric_name = ?1 ORDER BY labels",
        [metric_name]
      )

    series =
      Enum.map(rows, fn [labels_str] ->
        %{labels: decode_labels(labels_str)}
      end)

    {:ok, series}
  end

  @doc """
  List distinct values for a specific label key across all series of a metric.

  Returns `{:ok, ["web-1", "web-2", ...]}`.
  """
  def label_values(store, metric_name, label_key) do
    db = :"#{store}_db"

    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
        "SELECT labels FROM series WHERE metric_name = ?1",
        [metric_name]
      )

    values =
      rows
      |> Enum.map(fn [labels_str] -> decode_labels(labels_str) end)
      |> Enum.flat_map(fn labels -> Map.get(labels, label_key) |> List.wrap() end)
      |> Enum.uniq()
      |> Enum.sort()

    {:ok, values}
  end

  @doc """
  Register metadata for a metric (type, unit, description).

  ## Parameters

    * `store` - The store name
    * `metric_name` - The metric name
    * `metric_type` - One of `:gauge`, `:counter`, `:histogram`
    * `opts` - Optional:
      * `:unit` - Unit string (e.g., "%", "bytes", "ms")
      * `:description` - Human-readable description
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

  ## Parameters

    * `store` - The store name
    * `timestamp` - Unix timestamp in seconds
    * `title` - Short annotation title
    * `opts` - Optional:
      * `:description` - Longer description
      * `:tags` - List of tag strings

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

  ## Options

    * `:tags` - Filter by tags (list of strings, matches if any tag present)

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

  @doc """
  Delete an annotation by ID.
  """
  def delete_annotation(store, id) do
    db = :"#{store}_db"
    TimelessMetrics.DB.write(db, "DELETE FROM annotations WHERE id = ?1", [id])
    :ok
  end

  @doc """
  Create an alert rule.

  ## Required options

    * `:name` - Alert name
    * `:metric` - Metric name to monitor
    * `:condition` - `:above` or `:below`
    * `:threshold` - Numeric threshold

  ## Optional

    * `:labels` - Label filter map (default: all series)
    * `:duration` - Seconds value must breach before firing (default: 0)
    * `:aggregate` - Aggregate function (default: :avg)
    * `:webhook_url` - URL to POST on state transitions

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
  Get the schema configuration for a store.
  """
  def get_schema(store) do
    :persistent_term.get({TimelessMetrics, store, :schema}, TimelessMetrics.Schema.default())
  end

  @doc """
  Forecast future values for matching series.

  ## Options

    * `:from` - Training data start timestamp (required)
    * `:to` - Training data end timestamp (default: now)
    * `:horizon` - Seconds to forecast ahead (required)
    * `:bucket` - Bucket size: `{n, :seconds}`, `:minute`, `:hour` (default: `{300, :seconds}`)
    * `:aggregate` - Aggregate for bucketing training data (default: `:avg`)

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

  ## Options

    * `:from` - Start timestamp (required)
    * `:to` - End timestamp (default: now)
    * `:bucket` - Bucket size (default: `{300, :seconds}`)
    * `:aggregate` - Aggregate for bucketing (default: `:avg`)
    * `:sensitivity` - `:low`, `:medium`, or `:high` (default: `:medium`)

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

  # --- Internals ---

  defp buffer_shard_count(store) do
    :persistent_term.get({TimelessMetrics, store, :shard_count})
  end

  defp group_and_write_shards(resolved, store, shard_count) do
    by_shard = Enum.group_by(resolved, fn {sid, _, _} -> rem(abs(sid), shard_count) end)

    if map_size(by_shard) > 1 do
      by_shard
      |> Enum.map(fn {shard_idx, points} ->
        Task.async(fn ->
          TimelessMetrics.Buffer.write_bulk(:"#{store}_shard_#{shard_idx}", points)
        end)
      end)
      |> Task.await_many()
    else
      Enum.each(by_shard, fn {shard_idx, points} ->
        TimelessMetrics.Buffer.write_bulk(:"#{store}_shard_#{shard_idx}", points)
      end)
    end

    :ok
  end

  defp find_matching_series(store, metric_name, label_filter) do
    db = :"#{store}_db"

    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
        "SELECT id, labels FROM series WHERE metric_name = ?1",
        [metric_name]
      )

    rows
    |> Enum.map(fn [id, labels_str] -> {id, decode_labels(labels_str)} end)
    |> Enum.filter(fn {_id, labels} ->
      Enum.all?(label_filter, fn {k, v} -> Map.get(labels, k) == v end)
    end)
  end

  defp copy_dir_files(src_dir, dst_dir) do
    case File.ls(src_dir) do
      {:ok, files} ->
        File.mkdir_p!(dst_dir)

        Enum.reduce(files, 0, fn f, acc ->
          src = Path.join(src_dir, f)
          dst = Path.join(dst_dir, f)
          File.cp!(src, dst)

          case File.stat(dst) do
            {:ok, %{size: s}} -> acc + s
            _ -> acc
          end
        end)

      _ ->
        0
    end
  end

  defp dir_file_bytes(dir) do
    case File.ls(dir) do
      {:ok, files} ->
        Enum.reduce(files, 0, fn f, acc ->
          case File.stat(Path.join(dir, f)) do
            {:ok, %{size: size}} -> acc + size
            _ -> acc
          end
        end)

      _ ->
        0
    end
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
end
