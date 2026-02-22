defmodule TimelessMetrics do
  @moduledoc """
  Embedded time series storage for Elixir.

  Uses per-series actor processes with Gorilla compression and SQLite
  for fast, compact metric storage with daily rollups and configurable retention.

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

  alias TimelessMetrics.Actor.Engine

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
    Engine.write(store, metric_name, labels, value, opts)
  end

  @doc """
  Write a batch of metric points.

  Each entry is a tuple of `{metric_name, labels, value}` or
  `{metric_name, labels, value, timestamp}`.
  """
  def write_batch(store, entries) do
    Engine.write_batch(store, entries)
  end

  @doc """
  Query raw time series points for a single series (exact label match).

  ## Options

    * `:from` - Start timestamp (unix seconds, default: 0)
    * `:to` - End timestamp (unix seconds, default: now)

  Returns `{:ok, [{timestamp, value}, ...]}`.
  """
  def query(store, metric_name, labels, opts \\ []) do
    Engine.query(store, metric_name, labels, opts)
  end

  @doc """
  Query raw points across multiple series matching a label filter.

  Returns `{:ok, [%{labels: %{...}, points: [{ts, val}, ...]}, ...]}`.
  """
  def query_multi(store, metric_name, label_filter \\ %{}, opts \\ []) do
    Engine.query_multi(store, metric_name, label_filter, opts)
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
    Engine.query_aggregate(store, metric_name, labels, opts)
  end

  @doc """
  Query with aggregation across multiple series matching a label filter.

  Returns `{:ok, [%{labels: %{...}, data: [{bucket_ts, agg_value}, ...]}, ...]}`.
  """
  def query_aggregate_multi(store, metric_name, label_filter \\ %{}, opts) do
    Engine.query_aggregate_multi(store, metric_name, label_filter, opts)
  end

  @doc """
  Query with cross-series aggregation, grouping results by a label key.

  Returns `{:ok, [%{group: %{"hostname" => "host_0"}, data: [{ts, val}]}, ...]}`.
  """
  def query_aggregate_grouped(store, metric_name, label_filter \\ %{}, opts) do
    Engine.query_aggregate_grouped(store, metric_name, label_filter, opts)
  end

  @doc """
  Query with cross-series aggregation across multiple metrics, with group-by.

  Returns `{:ok, [%{group: %{...}, data: [{ts, val}]}, ...]}`.
  """
  def query_aggregate_grouped_metrics(store, metric_names, label_filter \\ %{}, opts)
      when is_list(metric_names) do
    Engine.query_aggregate_grouped_metrics(store, metric_names, label_filter, opts)
  end

  @doc """
  Query with aggregation and threshold filtering.

  Returns `{:ok, [%{labels: %{...}, data: [{ts, val}]}, ...]}`.
  """
  def query_aggregate_multi_filtered(store, metric_name, label_filter \\ %{}, opts) do
    Engine.query_aggregate_multi_filtered(store, metric_name, label_filter, opts)
  end

  @doc """
  Sort results by a value function and take top N.
  """
  def top_n(results, n, order_fn \\ &last_value/1) do
    results
    |> Enum.sort_by(order_fn, :desc)
    |> Enum.take(n)
  end

  defp last_value(%{data: []}), do: 0.0
  defp last_value(%{data: data}), do: data |> List.last() |> elem(1)

  @doc """
  Query with aggregation across multiple metric names matching a label filter.

  Returns `{:ok, [%{metric: name, labels: %{...}, data: [{ts, val}, ...]}, ...]}`.
  """
  def query_aggregate_multi_metrics(store, metric_names, label_filter \\ %{}, opts)
      when is_list(metric_names) do
    Engine.query_aggregate_multi_metrics(store, metric_names, label_filter, opts)
  end

  @doc """
  Query pre-computed daily rollup data.

  Returns `{:ok, [%{bucket: ts, avg: v, min: v, max: v, count: n, sum: v, last: v}, ...]}`.
  """
  def query_daily(store, metric_name, labels, from, to) do
    Engine.query_daily(store, metric_name, labels, from, to)
  end

  @doc """
  Get the latest value for a series.

  Returns `{:ok, {timestamp, value}}` or `{:ok, nil}`.
  """
  def latest(store, metric_name, labels) do
    Engine.latest(store, metric_name, labels)
  end

  @doc """
  Get the latest value for ALL series matching a metric name and label filter.

  Returns `{:ok, [%{labels: %{...}, timestamp: ts, value: val}, ...]}`.
  """
  def latest_multi(store, metric_name, label_filter \\ %{}) do
    Engine.latest_multi(store, metric_name, label_filter)
  end

  @doc """
  Force flush all buffered data to disk.
  """
  def flush(store) do
    Engine.flush(store)
  end

  @doc """
  Create a consistent online backup.

  Returns `{:ok, %{path: target_dir, files: [filenames], total_bytes: n}}`.
  """
  def backup(store, target_dir) do
    Engine.backup(store, target_dir)
  end

  @doc """
  Get store info and statistics.
  """
  def info(store) do
    Engine.info(store)
  end

  @doc """
  Force a daily rollup run.
  """
  def rollup(store) do
    TimelessMetrics.Actor.Rollup.run(:"#{store}_rollup")
  end

  @doc """
  Force retention enforcement now.
  """
  def enforce_retention(store) do
    TimelessMetrics.Actor.Retention.enforce(:"#{store}_retention")
  end

  @doc """
  List all distinct metric names in the store.

  Returns `{:ok, ["cpu_usage", "mem_usage", ...]}`.
  """
  def list_metrics(store) do
    Engine.list_metrics(store)
  end

  @doc """
  List all series for a given metric name.

  Returns `{:ok, [%{labels: %{"host" => "web-1"}, ...}, ...]}`.
  """
  def list_series(store, metric_name) do
    Engine.list_series(store, metric_name)
  end

  @doc """
  List distinct values for a specific label key across all series of a metric.

  Returns `{:ok, ["web-1", "web-2", ...]}`.
  """
  def label_values(store, metric_name, label_key) do
    Engine.label_values(store, metric_name, label_key)
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
end
