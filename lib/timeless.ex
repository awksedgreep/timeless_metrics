defmodule Timeless do
  @moduledoc """
  Embedded time series storage for Elixir.

  Combines Gorilla compression with SQLite for fast, compact metric storage
  with automatic rollups and configurable retention.

  ## Quick Start

      # Add to your supervision tree
      children = [
        {Timeless, name: :metrics, data_dir: "/tmp/metrics"}
      ]

      # Write metrics
      Timeless.write(:metrics, "cpu_usage", %{"host" => "web-1"}, 73.2)

      # Query
      Timeless.query(:metrics, "cpu_usage", %{"host" => "web-1"},
        from: System.os_time(:second) - 3600,
        to: System.os_time(:second)
      )
  """

  @doc "Start a Timeless instance as part of a supervision tree."
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {Timeless.Supervisor, :start_link, [opts]},
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
    series_id = Timeless.SeriesRegistry.get_or_create(registry, metric_name, labels)
    shard_count = buffer_shard_count(store)
    shard_idx = rem(abs(series_id), shard_count)
    shard_name = :"#{store}_shard_#{shard_idx}"
    Timeless.Buffer.write(shard_name, series_id, timestamp, value)
  end

  @doc """
  Write a batch of metric points.

  Each entry is a tuple of `{metric_name, labels, value}` or
  `{metric_name, labels, value, timestamp}`.
  """
  def write_batch(store, entries) do
    registry = :"#{store}_registry"
    shard_count = buffer_shard_count(store)

    Enum.each(entries, fn
      {metric_name, labels, value} ->
        timestamp = System.os_time(:second)
        series_id = Timeless.SeriesRegistry.get_or_create(registry, metric_name, labels)
        shard_idx = rem(abs(series_id), shard_count)
        shard_name = :"#{store}_shard_#{shard_idx}"
        Timeless.Buffer.write(shard_name, series_id, timestamp, value)

      {metric_name, labels, value, timestamp} ->
        series_id = Timeless.SeriesRegistry.get_or_create(registry, metric_name, labels)
        shard_idx = rem(abs(series_id), shard_count)
        shard_name = :"#{store}_shard_#{shard_idx}"
        Timeless.Buffer.write(shard_name, series_id, timestamp, value)
    end)
  end

  @doc """
  Query raw time series points for a single series (exact label match).

  ## Options

    * `:from` - Start timestamp (unix seconds, default: 0)
    * `:to` - End timestamp (unix seconds, default: now)

  Returns `{:ok, [{timestamp, value}, ...]}`.
  """
  def query(store, metric_name, labels, opts \\ []) do
    db = :"#{store}_db"
    registry = :"#{store}_registry"
    series_id = Timeless.SeriesRegistry.get_or_create(registry, metric_name, labels)
    Timeless.Query.raw(db, series_id, opts)
  end

  @doc """
  Query raw points across multiple series matching a label filter.

  `label_filter` is a map of labels that must be present. An empty map
  matches all series for the given metric.

  Returns `{:ok, [%{labels: %{...}, points: [{ts, val}, ...]}, ...]}`.
  """
  def query_multi(store, metric_name, label_filter \\ %{}, opts \\ []) do
    db = :"#{store}_db"
    matching = find_matching_series(store, metric_name, label_filter)

    results =
      Enum.map(matching, fn {series_id, labels} ->
        {:ok, points} = Timeless.Query.raw(db, series_id, opts)
        %{labels: labels, points: points}
      end)
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
    db = :"#{store}_db"
    registry = :"#{store}_registry"
    schema = get_schema(store)
    series_id = Timeless.SeriesRegistry.get_or_create(registry, metric_name, labels)
    Timeless.Query.aggregate(db, series_id, Keyword.put(opts, :schema, schema))
  end

  @doc """
  Query with aggregation across multiple series matching a label filter.

  Returns `{:ok, [%{labels: %{...}, data: [{bucket_ts, agg_value}, ...]}, ...]}`.
  """
  def query_aggregate_multi(store, metric_name, label_filter \\ %{}, opts) do
    db = :"#{store}_db"
    schema = get_schema(store)
    matching = find_matching_series(store, metric_name, label_filter)

    results =
      Enum.map(matching, fn {series_id, labels} ->
        {:ok, buckets} = Timeless.Query.aggregate(db, series_id, Keyword.put(opts, :schema, schema))
        %{labels: labels, data: buckets}
      end)
      |> Enum.reject(fn %{data: d} -> d == [] end)

    {:ok, results}
  end

  @doc """
  Read pre-computed rollup data directly from a tier.

  Returns `{:ok, [%{bucket: ts, avg: v, min: v, max: v, count: n, sum: v, last: v}, ...]}`.
  """
  def query_tier(store, tier_name, metric_name, labels, opts \\ []) do
    db = :"#{store}_db"
    registry = :"#{store}_registry"
    series_id = Timeless.SeriesRegistry.get_or_create(registry, metric_name, labels)
    Timeless.Query.read_tier(db, tier_name, series_id, opts)
  end

  @doc """
  Get the latest value for a series.

  Returns `{:ok, {timestamp, value}}` or `{:ok, nil}`.
  """
  def latest(store, metric_name, labels) do
    db = :"#{store}_db"
    registry = :"#{store}_registry"
    schema = get_schema(store)
    series_id = Timeless.SeriesRegistry.get_or_create(registry, metric_name, labels)
    Timeless.Query.latest(db, series_id, schema: schema)
  end

  @doc """
  Force flush all buffered data to disk.

  Useful for testing or before shutdown.
  """
  def flush(store) do
    builder = :"#{store}_builder"

    # Flush all buffer shards first
    shard_count = buffer_shard_count(store)

    for i <- 0..(shard_count - 1) do
      shard = :"#{store}_shard_#{i}"
      send(shard, :flush)
    end

    # Give shards a moment to drain to the builder
    Process.sleep(100)

    # Flush the segment builder
    Timeless.SegmentBuilder.flush(builder)
  end

  @doc """
  Get store info and statistics.

  Returns a map with raw segment stats, rollup tier stats, buffer sizes,
  watermark positions, and storage info.
  """
  def info(store) do
    db = :"#{store}_db"
    schema = get_schema(store)

    {:ok, [[series_count]]} = Timeless.DB.read(db, "SELECT COUNT(*) FROM series")
    {:ok, [[segment_count]]} = Timeless.DB.read(db, "SELECT COUNT(*) FROM raw_segments")
    {:ok, [[total_points]]} = Timeless.DB.read(db, "SELECT COALESCE(SUM(point_count), 0) FROM raw_segments")
    {:ok, [[raw_bytes]]} = Timeless.DB.read(db, "SELECT COALESCE(SUM(length(data)), 0) FROM raw_segments")

    {:ok, [[storage_bytes]]} =
      Timeless.DB.read(db, "SELECT page_count * page_size FROM pragma_page_count, pragma_page_size")

    {:ok, oldest} = Timeless.DB.read(db, "SELECT MIN(start_time) FROM raw_segments")
    {:ok, newest} = Timeless.DB.read(db, "SELECT MAX(end_time) FROM raw_segments")

    oldest_ts = case oldest do
      [[nil]] -> nil
      [[ts]] -> ts
    end

    newest_ts = case newest do
      [[nil]] -> nil
      [[ts]] -> ts
    end

    # Tier stats
    tier_stats =
      Enum.map(schema.tiers, fn tier ->
        {:ok, [[count]]} = Timeless.DB.read(db, "SELECT COUNT(*) FROM #{tier.table_name}")

        {:ok, watermark_rows} =
          Timeless.DB.read(db, "SELECT last_bucket FROM _watermarks WHERE tier = ?1", [
            to_string(tier.name)
          ])

        watermark = case watermark_rows do
          [[w]] -> w
          [] -> 0
        end

        retention_label =
          if tier.retention_seconds == :forever,
            do: "forever",
            else: "#{div(tier.retention_seconds, 86_400)}d"

        {tier.name, %{
          rows: count,
          resolution_seconds: tier.resolution_seconds,
          retention: retention_label,
          watermark: watermark
        }}
      end)
      |> Map.new()

    # Buffer sizes
    shard_count = buffer_shard_count(store)

    buffer_total =
      Enum.sum(
        for i <- 0..(shard_count - 1) do
          Timeless.Buffer.buffer_size(:"#{store}_shard_#{i}")
        end
      )

    bytes_per_point =
      if total_points > 0, do: Float.round(raw_bytes / total_points, 2), else: 0.0

    %{
      series_count: series_count,
      segment_count: segment_count,
      total_points: total_points,
      raw_compressed_bytes: raw_bytes,
      bytes_per_point: bytes_per_point,
      storage_bytes: storage_bytes,
      oldest_timestamp: oldest_ts,
      newest_timestamp: newest_ts,
      buffer_points: buffer_total,
      buffer_shards: shard_count,
      tiers: tier_stats,
      raw_retention: schema.raw_retention_seconds,
      db_path: Timeless.DB.db_path(db)
    }
  end

  @doc """
  Force a rollup of all tiers (or a specific tier).
  """
  def rollup(store, tier \\ :all) do
    Timeless.Rollup.run(:"#{store}_rollup", tier)
  end

  @doc """
  Force a late-arrival catch-up scan.

  Re-processes a lookback window behind each tier's watermark to pick up
  data points that arrived after rollup had already advanced past their timestamps.
  """
  def catch_up(store) do
    Timeless.Rollup.catch_up(:"#{store}_rollup")
  end

  @doc """
  Force retention enforcement now.
  """
  def enforce_retention(store) do
    Timeless.Retention.enforce(:"#{store}_retention")
  end

  @doc """
  List all distinct metric names in the store.

  Returns `{:ok, ["cpu_usage", "mem_usage", ...]}`.
  """
  def list_metrics(store) do
    db = :"#{store}_db"
    {:ok, rows} = Timeless.DB.read(db, "SELECT DISTINCT metric_name FROM series ORDER BY metric_name")
    {:ok, Enum.map(rows, fn [name] -> name end)}
  end

  @doc """
  List all series for a given metric name.

  Returns `{:ok, [%{labels: %{"host" => "web-1"}, ...}, ...]}`.
  """
  def list_series(store, metric_name) do
    db = :"#{store}_db"

    {:ok, rows} =
      Timeless.DB.read(
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
      Timeless.DB.read(
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

    Timeless.DB.write(
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
      Timeless.DB.read(
        db,
        "SELECT metric_type, unit, description FROM metric_metadata WHERE metric_name = ?1",
        [metric_name]
      )

    case rows do
      [[type, unit, desc]] ->
        {:ok, %{type: String.to_existing_atom(type), unit: unit, description: desc}}

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
      Timeless.DB.write_transaction(db, fn conn ->
        Timeless.DB.execute(
          conn,
          "INSERT INTO annotations (timestamp, title, description, tags, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
          [timestamp, title, description, tags, created_at]
        )

        {:ok, [[id]]} =
          Timeless.DB.execute(conn, "SELECT last_insert_rowid()", [])

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
      Timeless.DB.read(
        db,
        "SELECT id, timestamp, title, description, tags FROM annotations WHERE timestamp >= ?1 AND timestamp <= ?2 ORDER BY timestamp",
        [from, to]
      )

    results =
      rows
      |> Enum.map(fn [id, ts, title, desc, tags_str] ->
        tags = if tags_str && tags_str != "", do: String.split(tags_str, ",", trim: true), else: []
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
    Timeless.DB.write(db, "DELETE FROM annotations WHERE id = ?1", [id])
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
    Timeless.Alert.create_rule(db, opts)
  end

  @doc "List all alert rules with current state."
  def list_alerts(store) do
    db = :"#{store}_db"
    Timeless.Alert.list_rules(db)
  end

  @doc "Delete an alert rule."
  def delete_alert(store, rule_id) do
    db = :"#{store}_db"
    Timeless.Alert.delete_rule(db, rule_id)
  end

  @doc "Evaluate all alert rules against current data."
  def evaluate_alerts(store) do
    Timeless.Alert.evaluate(store)
  end

  @doc """
  Get the schema configuration for a store.
  """
  def get_schema(store) do
    :persistent_term.get({Timeless, store, :schema}, Timeless.Schema.default())
  end

  # --- Internals ---

  defp buffer_shard_count(store) do
    count_shards(store, 0)
  end

  defp count_shards(store, n) do
    case Process.whereis(:"#{store}_shard_#{n}") do
      nil -> max(n, 1)
      _pid -> count_shards(store, n + 1)
    end
  end

  defp find_matching_series(store, metric_name, label_filter) do
    db = :"#{store}_db"

    {:ok, rows} =
      Timeless.DB.read(
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
