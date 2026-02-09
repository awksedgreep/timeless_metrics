defmodule Timeless.HTTP do
  require Logger

  @moduledoc """
  Optional HTTP ingest interface compatible with VictoriaMetrics JSON line import format.

  ## Usage

  Add to your supervision tree alongside Timeless:

      children = [
        {Timeless, name: :metrics, data_dir: "/var/lib/metrics"},
        {Timeless.HTTP, store: :metrics, port: 8428}
      ]

  ## Endpoints

  ### Ingest
    * `POST /api/v1/import` - VictoriaMetrics JSON line import

  ### Query
    * `GET /api/v1/export` - Export raw points in VM JSON line format
    * `GET /api/v1/query` - Latest value for a series
    * `GET /api/v1/query_range` - Range query with bucketed aggregation

  ### Charts
    * `GET /chart` - SVG line chart, embeddable via `<img>` tag

  ### Operational
    * `GET /health` - Health check with store stats

  ## Query Parameters

  All query endpoints accept:
    * `metric` - metric name (required)
    * Any other param becomes a label filter (e.g. `?metric=cpu_usage&host=web-1`)

  Range endpoints also accept:
    * `start` or `from` - start timestamp (unix seconds, default: 1 hour ago)
    * `end` or `to` - end timestamp (unix seconds, default: now)

  `/api/v1/query_range` also accepts:
    * `step` - bucket size in seconds (default: 60)
    * `aggregate` - one of: avg, min, max, sum, count, last, first (default: avg)

  ## VictoriaMetrics JSON Line Format

  Each line is a JSON object:

      {"metric":{"__name__":"cpu_usage","host":"web-1"},"values":[73.2,74.1],"timestamps":[1700000000,1700000060]}

  The `__name__` field is the metric name; all other fields in `metric` become labels.
  `values` and `timestamps` are parallel arrays.

  ## Vector Configuration

      [sinks.metricstore]
      type = "http"
      inputs = ["metrics_transform"]
      uri = "http://localhost:8428/api/v1/import"
      encoding.codec = "text"
      framing.method = "newline_delimited"
  """

  use Plug.Router

  @max_body_bytes 10 * 1024 * 1024

  plug :match
  plug :authenticate
  plug :dispatch

  def child_spec(opts) do
    store = Keyword.fetch!(opts, :store)
    port = Keyword.get(opts, :port, 8428)
    bearer_token = Keyword.get(opts, :bearer_token)
    plug_opts = [store: store, bearer_token: bearer_token]

    %{
      id: {__MODULE__, store},
      start: {Bandit, :start_link, [[plug: {__MODULE__, plug_opts}, port: port]]},
      type: :supervisor
    }
  end

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, opts) do
    conn
    |> Plug.Conn.put_private(:timeless, Keyword.get(opts, :store))
    |> Plug.Conn.put_private(:timeless_token, Keyword.get(opts, :bearer_token))
    |> super(opts)
  end

  # Bearer token authentication plug.
  # Skips auth when no token is configured (backwards compatible).
  # Exempts /health for load balancers and monitoring.
  defp authenticate(%{request_path: "/health"} = conn, _opts), do: conn

  defp authenticate(conn, _opts) do
    case conn.private[:timeless_token] do
      nil -> conn
      expected -> check_token(conn, expected)
    end
  end

  defp check_token(conn, expected) do
    case extract_token(conn) do
      nil ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(401, ~s({"error":"unauthorized"}))
        |> halt()

      token ->
        if Plug.Crypto.secure_compare(token, expected) do
          conn
        else
          conn
          |> put_resp_content_type("application/json")
          |> send_resp(403, ~s({"error":"forbidden"}))
          |> halt()
        end
    end
  end

  defp extract_token(conn) do
    case Plug.Conn.get_req_header(conn, "authorization") do
      ["Bearer " <> token] ->
        String.trim(token)

      _ ->
        # Fallback: ?token= query param for browser access (dashboard, charts)
        conn = Plug.Conn.fetch_query_params(conn)
        conn.query_params["token"]
    end
  end

  # VictoriaMetrics JSON line import
  post "/api/v1/import" do
    store = conn.private.timeless

    case Plug.Conn.read_body(conn, length: @max_body_bytes) do
      {:ok, body, conn} ->
        {count, errors, error_samples} = ingest_json_lines(store, body)

        :telemetry.execute(
          [:timeless, :http, :import],
          %{sample_count: count, error_count: errors},
          %{store: store}
        )

        if errors > 0 do
          conn
          |> put_resp_content_type("application/json")
          |> send_resp(200, Jason.encode!(%{
            samples: count,
            errors: errors,
            failed_lines: error_samples
          }))
        else
          send_resp(conn, 204, "")
        end

      {:more, _partial, conn} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(413, Jason.encode!(%{error: "body too large", max_bytes: @max_body_bytes}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: to_string(reason)}))
    end
  end

  # Health check with store stats
  get "/health" do
    store = conn.private.timeless
    info = Timeless.info(store)

    body =
      Jason.encode!(%{
        status: "ok",
        series: info.series_count,
        points: info.total_points,
        storage_bytes: info.storage_bytes,
        buffer_points: info.buffer_points,
        bytes_per_point: info.bytes_per_point
      })

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, body)
  end

  # Online backup — creates consistent snapshot of all databases
  post "/api/v1/backup" do
    store = conn.private.timeless

    parsed_path =
      case Plug.Conn.read_body(conn, length: 64_000) do
        {:ok, "", _} ->
          nil

        {:ok, body, _} ->
          case Jason.decode(body) do
            {:ok, %{"path" => path}} when is_binary(path) and path != "" -> path
            _ -> nil
          end

        _ ->
          nil
      end

    target_dir =
      parsed_path || default_backup_dir(store)

    {:ok, result} = Timeless.backup(store, target_dir)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{
      status: "ok",
      path: result.path,
      files: result.files,
      total_bytes: result.total_bytes
    }))
  end

  # Export raw points in VictoriaMetrics JSON line format (multi-series)
  get "/api/v1/export" do
    store = conn.private.timeless
    conn = Plug.Conn.fetch_query_params(conn)

    case extract_query_params(conn.query_params) do
      {:ok, metric, labels, from, to} ->
        {:ok, results} = Timeless.query_multi(store, metric, labels, from: from, to: to)

        body =
          results
          |> Enum.map(fn %{labels: l, points: pts} ->
            {timestamps, values} = Enum.unzip(pts)

            Jason.encode!(%{
              metric: Map.put(l, "__name__", metric),
              values: values,
              timestamps: timestamps
            })
          end)
          |> Enum.join("\n")

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, body)

      {:error, msg} ->
        json_error(conn, 400, msg)
    end
  end

  # Latest value for matching series
  get "/api/v1/query" do
    store = conn.private.timeless
    conn = Plug.Conn.fetch_query_params(conn)

    case extract_metric_and_labels(conn.query_params) do
      {:ok, metric, labels} ->
        {:ok, results} = Timeless.query_multi(store, metric, labels)

        data =
          results
          |> Enum.flat_map(fn %{labels: l, points: pts} ->
            case List.last(Enum.sort_by(pts, &elem(&1, 0))) do
              {ts, val} -> [%{labels: l, timestamp: ts, value: val}]
              nil -> []
            end
          end)

        body =
          case data do
            [single] -> Jason.encode!(single)
            multiple -> Jason.encode!(%{data: multiple})
          end

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, body)

      {:error, msg} ->
        json_error(conn, 400, msg)
    end
  end

  # Range query with bucketed aggregation (multi-series)
  get "/api/v1/query_range" do
    store = conn.private.timeless
    conn = Plug.Conn.fetch_query_params(conn)

    case extract_query_params(conn.query_params) do
      {:ok, metric, labels, from, to} ->
        params = conn.query_params
        step = parse_int(params["step"], 60)
        agg = parse_aggregate(params["aggregate"])
        transform = Timeless.Transform.parse(params["transform"])

        {:ok, results} =
          Timeless.query_aggregate_multi(store, metric, labels,
            from: from,
            to: to,
            bucket: {step, :seconds},
            aggregate: agg,
            transform: transform
          )

        series =
          Enum.map(results, fn %{labels: l, data: buckets} ->
            %{labels: l, data: Enum.map(buckets, fn {ts, val} -> [ts, val] end)}
          end)

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{metric: metric, series: series}))

      {:error, msg} ->
        json_error(conn, 400, msg)
    end
  end

  # List all metric names
  get "/api/v1/label/__name__/values" do
    store = conn.private.timeless
    {:ok, metrics} = Timeless.list_metrics(store)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{status: "success", data: metrics}))
  end

  # List values for a specific label key
  get "/api/v1/label/:name/values" do
    store = conn.private.timeless
    conn = Plug.Conn.fetch_query_params(conn)
    label_name = conn.path_params["name"]
    metric = conn.query_params["metric"]

    if metric do
      {:ok, values} = Timeless.label_values(store, metric, label_name)

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, Jason.encode!(%{status: "success", data: values}))
    else
      json_error(conn, 400, "missing required parameter: metric")
    end
  end

  # List all series for a metric
  get "/api/v1/series" do
    store = conn.private.timeless
    conn = Plug.Conn.fetch_query_params(conn)

    case conn.query_params["metric"] do
      nil ->
        json_error(conn, 400, "missing required parameter: metric")

      metric ->
        {:ok, series} = Timeless.list_series(store, metric)

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{status: "success", data: series}))
    end
  end

  # Register or update metric metadata
  post "/api/v1/metadata" do
    store = conn.private.timeless

    case Plug.Conn.read_body(conn, length: 64_000) do
      {:ok, body, conn} ->
        case Jason.decode(body) do
          {:ok, %{"metric" => metric, "type" => type} = params} when type in ~w(gauge counter histogram) ->
            Timeless.register_metric(store, metric, String.to_existing_atom(type),
              unit: params["unit"],
              description: params["description"]
            )

            conn
            |> put_resp_content_type("application/json")
            |> send_resp(200, Jason.encode!(%{status: "ok"}))

          {:ok, %{"metric" => _}} ->
            json_error(conn, 400, "type must be one of: gauge, counter, histogram")

          _ ->
            json_error(conn, 400, "invalid JSON: requires metric and type fields")
        end

      {:error, reason} ->
        json_error(conn, 400, to_string(reason))
    end
  end

  # Get metric metadata
  get "/api/v1/metadata" do
    store = conn.private.timeless
    conn = Plug.Conn.fetch_query_params(conn)

    case conn.query_params["metric"] do
      nil ->
        json_error(conn, 400, "missing required parameter: metric")

      metric ->
        {:ok, meta} = Timeless.get_metadata(store, metric)

        if meta do
          conn
          |> put_resp_content_type("application/json")
          |> send_resp(200, Jason.encode!(%{metric: metric, type: meta.type, unit: meta.unit, description: meta.description}))
        else
          conn
          |> put_resp_content_type("application/json")
          |> send_resp(200, Jason.encode!(%{metric: metric, type: "gauge", unit: nil, description: nil}))
        end
    end
  end

  # Create an annotation
  post "/api/v1/annotations" do
    store = conn.private.timeless

    case Plug.Conn.read_body(conn, length: 64_000) do
      {:ok, body, conn} ->
        case Jason.decode(body) do
          {:ok, %{"title" => title} = params} ->
            timestamp = params["timestamp"] || System.os_time(:second)
            tags = params["tags"] || []
            description = params["description"]

            {:ok, id} =
              Timeless.annotate(store, timestamp, title,
                description: description,
                tags: tags
              )

            conn
            |> put_resp_content_type("application/json")
            |> send_resp(201, Jason.encode!(%{id: id, status: "created"}))

          _ ->
            json_error(conn, 400, "invalid JSON: requires title field")
        end

      {:error, reason} ->
        json_error(conn, 400, to_string(reason))
    end
  end

  # Query annotations in a time range
  get "/api/v1/annotations" do
    store = conn.private.timeless
    conn = Plug.Conn.fetch_query_params(conn)
    params = conn.query_params

    now = System.os_time(:second)
    from = parse_time(params["from"], now - 86_400)
    to = parse_time(params["to"], now)

    tag_filter =
      case params["tags"] do
        nil -> []
        tags_str -> String.split(tags_str, ",", trim: true)
      end

    {:ok, results} = Timeless.annotations(store, from, to, tags: tag_filter)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{data: results}))
  end

  # Delete an annotation
  delete "/api/v1/annotations/:id" do
    store = conn.private.timeless
    {id, _} = Integer.parse(conn.path_params["id"])
    Timeless.delete_annotation(store, id)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{status: "deleted"}))
  end

  # Create an alert rule
  post "/api/v1/alerts" do
    store = conn.private.timeless

    case Plug.Conn.read_body(conn, length: 64_000) do
      {:ok, body, conn} ->
        case Jason.decode(body) do
          {:ok, %{"name" => name, "metric" => metric, "condition" => cond_str, "threshold" => threshold} = params}
          when cond_str in ~w(above below) and is_number(threshold) ->
            opts = [
              name: name,
              metric: metric,
              condition: String.to_existing_atom(cond_str),
              threshold: threshold,
              labels: params["labels"] || %{},
              duration: params["duration"] || 0,
              aggregate: String.to_existing_atom(params["aggregate"] || "avg"),
              webhook_url: params["webhook_url"]
            ]

            {:ok, id} = Timeless.create_alert(store, opts)

            conn
            |> put_resp_content_type("application/json")
            |> send_resp(201, Jason.encode!(%{id: id, status: "created"}))

          _ ->
            json_error(conn, 400, "requires: name, metric, condition (above/below), threshold (number)")
        end

      {:error, reason} ->
        json_error(conn, 400, to_string(reason))
    end
  end

  # List all alert rules with state
  get "/api/v1/alerts" do
    store = conn.private.timeless
    {:ok, rules} = Timeless.list_alerts(store)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{data: rules}))
  end

  # Delete an alert rule
  delete "/api/v1/alerts/:id" do
    store = conn.private.timeless
    {id, _} = Integer.parse(conn.path_params["id"])
    Timeless.delete_alert(store, id)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{status: "deleted"}))
  end

  # Forecast future values
  get "/api/v1/forecast" do
    store = conn.private.timeless
    conn = Plug.Conn.fetch_query_params(conn)

    case extract_query_params(conn.query_params) do
      {:ok, metric, labels, from, to} ->
        params = conn.query_params
        step = parse_int(params["step"], 300)
        horizon = parse_duration_param(params["horizon"], 3600)
        transform = Timeless.Transform.parse(params["transform"])

        {:ok, results} =
          Timeless.query_aggregate_multi(store, metric, labels,
            from: from,
            to: to,
            bucket: {step, :seconds},
            aggregate: :avg,
            transform: transform
          )

        forecasts =
          Enum.map(results, fn %{labels: l, data: data} ->
            case Timeless.Forecast.predict(data, horizon: horizon, bucket: step) do
              {:ok, predictions} ->
                %{
                  labels: l,
                  data: Enum.map(data, fn {ts, val} -> [ts, val] end),
                  forecast: Enum.map(predictions, fn {ts, val} -> [ts, val] end)
                }

              {:error, _} ->
                %{labels: l, data: Enum.map(data, fn {ts, val} -> [ts, val] end), forecast: []}
            end
          end)

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{metric: metric, series: forecasts}))

      {:error, msg} ->
        json_error(conn, 400, msg)
    end
  end

  # Anomaly detection
  get "/api/v1/anomalies" do
    store = conn.private.timeless
    conn = Plug.Conn.fetch_query_params(conn)

    case extract_query_params(conn.query_params) do
      {:ok, metric, labels, from, to} ->
        params = conn.query_params
        step = parse_int(params["step"], 300)
        sensitivity = parse_sensitivity(params["sensitivity"])
        transform = Timeless.Transform.parse(params["transform"])

        {:ok, results} =
          Timeless.query_aggregate_multi(store, metric, labels,
            from: from,
            to: to,
            bucket: {step, :seconds},
            aggregate: :avg,
            transform: transform
          )

        detections =
          Enum.map(results, fn %{labels: l, data: data} ->
            case Timeless.Anomaly.detect(data, sensitivity: sensitivity) do
              {:ok, analysis} -> %{labels: l, analysis: analysis}
              {:error, _} -> %{labels: l, analysis: []}
            end
          end)

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{metric: metric, series: detections}))

      {:error, msg} ->
        json_error(conn, 400, msg)
    end
  end

  # SVG chart — embeddable via <img src="http://host:port/chart?metric=cpu&host=web-1&from=-1h">
  # Optional: &forecast=1h for forecast overlay, &anomalies=medium for anomaly markers
  get "/chart" do
    store = conn.private.timeless
    conn = Plug.Conn.fetch_query_params(conn)
    params = conn.query_params

    case extract_chart_params(params) do
      {:ok, metric, labels, from, to, step, agg, width, height, theme} ->
        transform = Timeless.Transform.parse(params["transform"])

        {:ok, results} =
          Timeless.query_aggregate_multi(store, metric, labels,
            from: from,
            to: to,
            bucket: {step, :seconds},
            aggregate: agg,
            transform: transform
          )

        {:ok, annots} = Timeless.annotations(store, from, to)

        # Optional forecast overlay
        forecast_data =
          case params["forecast"] do
            nil ->
              []

            horizon_str ->
              horizon = parse_duration_param(horizon_str, 3600)

              case results do
                [%{data: data} | _] ->
                  case Timeless.Forecast.predict(data, horizon: horizon, bucket: step) do
                    {:ok, predictions} ->
                      last_point = List.last(data)
                      if last_point, do: [last_point | predictions], else: predictions

                    _ ->
                      []
                  end

                _ ->
                  []
              end
          end

        # Optional anomaly overlay
        anomaly_points =
          case params["anomalies"] do
            nil ->
              []

            sensitivity_str ->
              sensitivity = parse_sensitivity(sensitivity_str)

              results
              |> Enum.flat_map(fn %{data: data} ->
                case Timeless.Anomaly.detect(data, sensitivity: sensitivity) do
                  {:ok, analysis} ->
                    analysis
                    |> Enum.filter(& &1.anomaly)
                    |> Enum.map(fn a -> {a.timestamp, a.value} end)

                  _ ->
                    []
                end
              end)
          end

        svg =
          Timeless.Chart.render(metric, results,
            width: width,
            height: height,
            theme: theme,
            annotations: annots,
            forecast: forecast_data,
            anomalies: anomaly_points
          )

        conn
        |> put_resp_content_type("image/svg+xml")
        |> put_resp_header("cache-control", "public, max-age=60")
        |> send_resp(200, svg)

      {:error, msg} ->
        json_error(conn, 400, msg)
    end
  end

  # Prometheus text exposition format import
  # Each line: metric_name{label1="val1",label2="val2"} value [timestamp_ms]
  post "/api/v1/import/prometheus" do
    store = conn.private.timeless

    case Plug.Conn.read_body(conn, length: @max_body_bytes) do
      {:ok, body, conn} ->
        {count, errors, error_samples} = ingest_prometheus_text(store, body)

        :telemetry.execute(
          [:timeless, :http, :import],
          %{sample_count: count, error_count: errors},
          %{store: store, format: :prometheus}
        )

        if errors > 0 do
          conn
          |> put_resp_content_type("application/json")
          |> send_resp(200, Jason.encode!(%{
            samples: count,
            errors: errors,
            failed_lines: error_samples
          }))
        else
          send_resp(conn, 204, "")
        end

      {:more, _partial, conn} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(413, Jason.encode!(%{error: "body too large", max_bytes: @max_body_bytes}))

      {:error, reason} ->
        json_error(conn, 400, to_string(reason))
    end
  end

  # Prometheus-compatible query_range endpoint (for Grafana)
  get "/prometheus/api/v1/query_range" do
    store = conn.private.timeless
    conn = Plug.Conn.fetch_query_params(conn)
    params = conn.query_params

    case params["query"] do
      nil ->
        json_error(conn, 400, "missing required parameter: query")

      query ->
        {metric, labels} = parse_promql_simple(query)
        now = System.os_time(:second)

        start_ts = parse_prom_time(params["start"], now - 3600)
        end_ts = parse_prom_time(params["end"], now)
        step = parse_prom_step(params["step"], 60)

        {:ok, results} =
          Timeless.query_aggregate_multi(store, metric, labels,
            from: start_ts,
            to: end_ts,
            bucket: {step, :seconds},
            aggregate: :avg
          )

        # Format as Prometheus API response
        prom_results =
          Enum.map(results, fn %{labels: l, data: buckets} ->
            %{
              "metric" => Map.put(l, "__name__", metric),
              "values" =>
                Enum.map(buckets, fn {ts, val} ->
                  [ts, Float.to_string(val * 1.0)]
                end)
            }
          end)

        body =
          Jason.encode!(%{
            "status" => "success",
            "data" => %{
              "resultType" => "matrix",
              "result" => prom_results
            }
          })

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, body)
    end
  end

  # Dashboard — zero-dependency HTML overview page
  get "/" do
    store = conn.private.timeless
    conn = Plug.Conn.fetch_query_params(conn)
    params = conn.query_params

    from = params["from"] || "-1h"
    to = params["to"] || "now"
    filter = label_params(params)

    html =
      Timeless.Dashboard.render(
        store: store,
        from: from,
        to: to,
        filter: filter
      )

    conn
    |> put_resp_content_type("text/html")
    |> send_resp(200, html)
  end

  match _ do
    send_resp(conn, 404, "not found")
  end

  # --- Internals ---

  @reserved_params ~w(metric from to start end step aggregate width height label_key theme transform token forecast anomalies sensitivity horizon)

  defp extract_metric_and_labels(params) do
    case params["metric"] do
      nil -> {:error, "missing required parameter: metric"}
      metric -> {:ok, metric, label_params(params)}
    end
  end

  defp extract_query_params(params) do
    case extract_metric_and_labels(params) do
      {:ok, metric, labels} ->
        now = System.os_time(:second)
        from = parse_time(params["start"], parse_time(params["from"], now - 3600))
        to = parse_time(params["end"], parse_time(params["to"], now))
        {:ok, metric, labels, from, to}

      error ->
        error
    end
  end

  defp label_params(params) do
    params
    |> Map.drop(@reserved_params)
    |> Map.new(fn {k, v} -> {to_string(k), to_string(v)} end)
  end

  defp parse_int(nil, default), do: default
  defp parse_int(val, default) when is_binary(val) do
    case Integer.parse(val) do
      {n, _} -> n
      :error -> default
    end
  end
  defp parse_int(val, _default) when is_integer(val), do: val

  # Parse time values that can be absolute unix timestamps or relative durations
  defp parse_time(nil, default), do: default
  defp parse_time(val, default) when is_binary(val) do
    now = System.os_time(:second)

    case val do
      "-" <> rest -> now - parse_duration(rest, 0)
      "now" -> now
      _ -> parse_int(val, default)
    end
  end

  defp parse_duration(str, fallback) do
    case Integer.parse(str) do
      {n, "s"} -> n
      {n, "m"} -> n * 60
      {n, "h"} -> n * 3600
      {n, "d"} -> n * 86400
      {n, "w"} -> n * 604_800
      {n, ""} -> n
      _ -> fallback
    end
  end

  defp parse_sensitivity(nil), do: :medium
  defp parse_sensitivity("true"), do: :medium
  defp parse_sensitivity(s) when s in ~w(low medium high), do: String.to_existing_atom(s)
  defp parse_sensitivity(_), do: :medium

  defp parse_duration_param(nil, default), do: default

  defp parse_duration_param(val, default) when is_binary(val) do
    case Integer.parse(val) do
      {n, "s"} -> n
      {n, "m"} -> n * 60
      {n, "h"} -> n * 3600
      {n, "d"} -> n * 86400
      {n, ""} -> n
      _ -> default
    end
  end

  defp parse_aggregate(nil), do: :avg
  defp parse_aggregate(agg) when agg in ~w(avg min max sum count last first rate),
    do: String.to_existing_atom(agg)
  defp parse_aggregate(_), do: :avg

  defp extract_chart_params(params) do
    case extract_query_params(params) do
      {:ok, metric, labels, from, to} ->
        # Auto-compute step from time range if not specified
        range = to - from
        default_step = max(div(range, 200), 1)
        step = parse_int(params["step"], default_step)
        agg = parse_aggregate(params["aggregate"])
        width = parse_int(params["width"], 800)
        height = parse_int(params["height"], 300)
        theme = parse_theme(params["theme"])
        {:ok, metric, labels, from, to, step, agg, width, height, theme}

      error ->
        error
    end
  end

  defp default_backup_dir(store) do
    db_path = Timeless.DB.db_path(:"#{store}_db")
    data_dir = Path.dirname(db_path)
    Path.join([data_dir, "backups", to_string(System.os_time(:second))])
  end

  defp parse_theme("dark"), do: :dark
  defp parse_theme("light"), do: :light
  defp parse_theme(_), do: :auto

  defp json_error(conn, status, msg) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(%{error: msg}))
  end

  @max_error_samples 3

  defp ingest_json_lines(store, body) do
    {all_entries, errors, error_samples} =
      body
      |> String.split("\n", trim: true)
      |> Enum.reduce({[], 0, []}, fn line, {entries_acc, errors, samples} ->
        case parse_vm_line(line) do
          {:ok, entries} ->
            {[entries | entries_acc], errors, samples}

          :error ->
            samples =
              if length(samples) < @max_error_samples do
                [String.slice(line, 0, 200) | samples]
              else
                samples
              end

            {entries_acc, errors + 1, samples}
        end
      end)

    if errors > 0 do
      Logger.warning("Import: #{errors} line(s) failed to parse, sample: #{inspect(Enum.reverse(error_samples))}")
    end

    # One batch call for the entire body
    flat_entries = List.flatten(all_entries)

    if flat_entries != [] do
      Timeless.write_batch(store, flat_entries)
    end

    {length(flat_entries), errors, Enum.reverse(error_samples)}
  end

  defp parse_vm_line(line) do
    decoded =
      try do
        {:ok, :json.decode(line)}
      rescue
        _ -> :error
      end

    case decoded do
      {:ok, %{"metric" => metric_map, "values" => values, "timestamps" => timestamps}}
      when is_list(values) and is_list(timestamps) and length(values) == length(timestamps) ->
        {name, labels} = extract_metric(metric_map)

        entries =
          Enum.zip(timestamps, values)
          |> Enum.map(fn {ts, val} when is_number(val) and is_integer(ts) ->
            {name, labels, val * 1.0, ts}
          end)

        {:ok, entries}

      _ ->
        :error
    end
  end

  defp extract_metric(metric_map) do
    {name, labels} = Map.pop(metric_map, "__name__", "unknown")
    {name, labels}
  end

  # --- Prometheus text format parser ---
  # Handles: metric_name{label1="val1",label2="val2"} value [timestamp_ms]
  # Also handles: metric_name value [timestamp_ms] (no labels)
  # Skips comments (#) and empty lines.

  defp ingest_prometheus_text(store, body) do
    {all_entries, errors, error_samples} =
      body
      |> String.split("\n", trim: true)
      |> Enum.reduce({[], 0, []}, fn line, {entries_acc, errors, samples} ->
        line = String.trim(line)

        if line == "" or String.starts_with?(line, "#") do
          {entries_acc, errors, samples}
        else
          case parse_prometheus_line(line) do
            {:ok, metric, labels, value, timestamp} ->
              {[{metric, labels, value, timestamp} | entries_acc], errors, samples}

            :error ->
              samples =
                if length(samples) < @max_error_samples do
                  [String.slice(line, 0, 200) | samples]
                else
                  samples
                end

              {entries_acc, errors + 1, samples}
          end
        end
      end)

    if errors > 0 do
      Logger.warning("Prometheus import: #{errors} line(s) failed to parse, sample: #{inspect(Enum.reverse(error_samples))}")
    end

    if all_entries != [] do
      Timeless.write_batch(store, all_entries)
    end

    {length(all_entries), errors, Enum.reverse(error_samples)}
  end

  defp parse_prometheus_line(line) do
    # Binary matching — split metric{labels} from value [timestamp]
    case :binary.match(line, <<"{">>) do
      {brace_pos, 1} ->
        metric = :binary.part(line, 0, brace_pos)
        rest = :binary.part(line, brace_pos + 1, byte_size(line) - brace_pos - 1)

        case :binary.split(rest, <<"} ">>) do
          [labels_str, value_ts] ->
            labels = parse_prometheus_labels_bin(labels_str)
            parse_value_timestamp(String.trim(value_ts), metric, labels)

          _ ->
            :error
        end

      :nomatch ->
        # No labels: metric value [timestamp]
        case :binary.split(line, <<" ">>) do
          [metric, value_ts] ->
            parse_value_timestamp(String.trim(value_ts), metric, %{})

          _ ->
            :error
        end
    end
  end

  defp parse_value_timestamp(str, metric, labels) do
    case :binary.split(str, <<" ">>) do
      [value_str, ts_str] ->
        with {value, _} <- Float.parse(value_str),
             {ts_ms, _} <- Integer.parse(ts_str) do
          {:ok, metric, labels, value, div(ts_ms, 1000)}
        else
          _ -> :error
        end

      [value_str] ->
        case Float.parse(value_str) do
          {value, _} -> {:ok, metric, labels, value, System.os_time(:second)}
          _ -> :error
        end
    end
  end

  defp parse_prometheus_labels_bin(str) when byte_size(str) == 0, do: %{}

  defp parse_prometheus_labels_bin(str) do
    str
    |> :binary.split(<<",">>, [:global])
    |> Enum.reduce(%{}, fn pair, acc ->
      case :binary.split(pair, <<"=">>, [:global]) do
        [key, value] ->
          v = value |> String.trim() |> String.trim("\"")
          Map.put(acc, String.trim(key), v)

        _ ->
          acc
      end
    end)
  end

  # Simple PromQL parser — handles metric_name{label="value",...}
  defp parse_promql_simple(query) do
    query = String.trim(query)

    case Regex.run(~r/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?$/, query) do
      [_, metric, labels_str] ->
        labels = parse_promql_labels(labels_str)
        {metric, labels}

      [_, metric] ->
        {metric, %{}}

      _ ->
        {query, %{}}
    end
  end

  defp parse_promql_labels(""), do: %{}

  defp parse_promql_labels(str) do
    str
    |> String.split(",", trim: true)
    |> Enum.map(fn pair ->
      case Regex.run(~r/^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"([^"]*)"\s*$/, pair) do
        [_, k, v] -> {k, v}
        _ -> nil
      end
    end)
    |> Enum.reject(&is_nil/1)
    |> Map.new()
  end

  # Parse Prometheus time params — can be unix timestamps (float or int)
  defp parse_prom_time(nil, default), do: default

  defp parse_prom_time(val, default) when is_binary(val) do
    case Float.parse(val) do
      {ts, _} -> trunc(ts)
      :error -> default
    end
  end

  # Parse Prometheus step — can be integer seconds or duration string like "60s"
  defp parse_prom_step(nil, default), do: default

  defp parse_prom_step(val, default) when is_binary(val) do
    case Integer.parse(val) do
      {n, "s"} -> n
      {n, "m"} -> n * 60
      {n, "h"} -> n * 3600
      {n, ""} -> n
      _ -> default
    end
  end
end
