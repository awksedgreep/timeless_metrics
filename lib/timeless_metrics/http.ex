defmodule TimelessMetrics.HTTP do
  require Logger

  @moduledoc """
  Optional HTTP ingest interface compatible with VictoriaMetrics JSON line import format.

  ## Usage

  Add to your supervision tree alongside TimelessMetrics:

      children = [
        {TimelessMetrics, name: :metrics, data_dir: "/var/lib/metrics"},
        {TimelessMetrics.HTTP, store: :metrics, port: 8428}
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

  use Rocket.Router

  @max_body_bytes 10 * 1024 * 1024

  def child_spec(opts) do
    store = Keyword.fetch!(opts, :store)
    port = Keyword.get(opts, :port, 8428)
    bearer_token = Keyword.get(opts, :bearer_token)

    :persistent_term.put({__MODULE__, :config}, {store, bearer_token})

    %{
      id: {__MODULE__, store},
      start:
        {Rocket, :start_link, [[port: port, handler: __MODULE__, max_body: @max_body_bytes]]},
      type: :supervisor
    }
  end

  # --- Config access ---

  defp store, do: elem(:persistent_term.get({__MODULE__, :config}), 0)
  defp bearer_token, do: elem(:persistent_term.get({__MODULE__, :config}), 1)

  # --- Authentication ---
  # Returns :ok or :halt (response already sent).
  # Skips auth when no token is configured (backwards compatible).

  defp check_auth(req) do
    case bearer_token() do
      nil -> :ok
      expected -> verify_token(req, expected)
    end
  end

  defp verify_token(req, expected) do
    case extract_token(req) do
      nil ->
        json_resp(req, 401, %{error: "unauthorized"})
        :halt

      token ->
        if constant_time_compare(token, expected) do
          :ok
        else
          json_resp(req, 403, %{error: "forbidden"})
          :halt
        end
    end
  end

  defp extract_token(req) do
    auth =
      Rocket.Request.get_header(req, "Authorization") ||
        Rocket.Request.get_header(req, "authorization")

    case auth do
      "Bearer " <> token ->
        String.trim(token)

      _ ->
        {params, _} = Rocket.Request.query_params(req)
        params["token"]
    end
  end

  defp constant_time_compare(a, b) when byte_size(a) == byte_size(b) do
    :crypto.hash_equals(a, b)
  end

  defp constant_time_compare(_a, _b), do: false

  # --- Response helpers ---

  defp json_resp(req, status, term) do
    body = json_encode!(term)

    Rocket.Response.send_iodata(req, status, [{"content-type", "application/json"}], body)
  end

  defp json_error(req, status, msg) do
    json_resp(req, status, %{error: msg})
  end

  # Prometheus API error shape — clients (Grafana etc.) surface these instead
  # of rendering an empty chart.
  defp prom_error(req, status, error_type, msg) do
    json_resp(req, status, %{status: "error", errorType: error_type, error: msg})
  end

  # Rejected PromQL goes through the gap radar (Stats) so real traffic tells
  # us which unsupported constructs to implement next.
  defp promql_error(req, store, query, :parse, reason) do
    TimelessMetrics.Stats.record_promql_rejection(store, query, reason)
    prom_error(req, 400, "bad_data", "PromQL parse error: #{reason}")
  end

  defp promql_error(req, store, query, :execution, reason) do
    TimelessMetrics.Stats.record_promql_rejection(store, query, reason)
    prom_error(req, 422, "execution", reason)
  end

  # Prometheus API allows GET query params and POST form bodies interchangeably;
  # merge both so each handler serves either method.
  defp merged_params(req) do
    {query_params, _} = Rocket.Request.query_params(req)

    case Map.get(req, :body) do
      body when is_binary(body) and body != "" ->
        Map.merge(query_params, URI.decode_query(body))

      _ ->
        query_params
    end
  end

  defp html_resp(req, status, html) do
    Rocket.Response.send_iodata(req, status, [{"content-type", "text/html"}], html)
  end

  defp text_resp(req, status, text) do
    Rocket.Response.send_iodata(req, status, [{"content-type", "text/plain"}], text)
  end

  # --- Route Handlers ---

  # InfluxDB line protocol import (used by TSBS, compatible with VictoriaMetrics /write)
  # Format: measurement,tag=val,tag=val field=value timestamp_ns
  post "/write" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        TimelessMetrics.Stats.incr_http_imports(store)
        body = req.body

        {count, errors, error_samples} = ingest_influx_lines(store, body)
        TimelessMetrics.Stats.add_http_import_errors(store, errors)

        :telemetry.execute(
          [:timeless_metrics, :http, :import],
          %{sample_count: count, error_count: errors},
          %{store: store, format: :influx}
        )

        if errors > 0 do
          Logger.warning(
            "Influx import: #{errors} line(s) failed to parse, sample: #{inspect(error_samples)}"
          )

          json_resp(req, 200, %{
            samples: count,
            errors: errors,
            failed_lines: error_samples
          })
        else
          send_resp(req, 204)
        end
    end
  end

  # VictoriaMetrics JSON line import
  post "/api/v1/import" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        TimelessMetrics.Stats.incr_http_imports(store)
        body = req.body

        queue = :persistent_term.get({TimelessMetrics, store, :ingest_queue})
        TimelessMetrics.IngestWorker.enqueue(queue, body, :json)
        send_resp(req, 204)
    end
  end

  # Fast health check — no fan-out, just counters + registry size (no auth required)
  get "/health" do
    store = store()
    stats = TimelessMetrics.Stats.snapshot(store)
    {series_count, points, buffer_points} = health_counts(store, stats)

    json_resp(req, 200, %{
      status: "ok",
      series: series_count,
      points: points,
      buffer_points: buffer_points,
      queries: stats.queries,
      query_fast_path: stats.query_fast_path,
      query_slow_path: stats.query_slow_path
    })
  end

  defp health_counts(store, stats) do
    if :persistent_term.get({TimelessMetrics, store, :engine}, nil) == :rust do
      info = TimelessMetrics.info(store)
      {info.series_count, info.total_points, info.raw_buffer_points}
    else
      series_count = TimelessMetrics.SeriesRegistry.count(:"#{store}_registry")
      {series_count, stats.points_ingested, stats.points_ingested - stats.points_merged}
    end
  end

  # Full diagnostic info — expensive, fans out to all series (auth required)
  get "/health/detailed" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        info = TimelessMetrics.info(store)
        stats = TimelessMetrics.Stats.snapshot(store)

        json_resp(req, 200, %{
          status: "ok",
          series: info.series_count,
          points: info.total_points,
          storage_bytes: info.storage_bytes,
          buffer_points: info.raw_buffer_points,
          bytes_per_point: info.bytes_per_point,
          promql_rejected: stats.promql_rejected,
          promql_rejections: TimelessMetrics.Stats.promql_rejections(store)
        })
    end
  end

  # Online backup — creates consistent snapshot of all databases
  post "/api/v1/backup" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        body = req.body

        parsed_path =
          case body do
            "" ->
              nil

            _ ->
              case safe_json_decode(body) do
                %{"path" => path} when is_binary(path) and path != "" -> path
                _ -> nil
              end
          end

        target_dir = parsed_path || default_backup_dir(store)

        {:ok, info} = TimelessMetrics.backup(store, target_dir)

        json_resp(req, 200, %{
          status: "ok",
          path: info.path,
          files: info.files,
          total_bytes: info.total_bytes
        })
    end
  end

  # Export raw points in VictoriaMetrics JSON line format (multi-series)
  get "/api/v1/export" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        TimelessMetrics.Stats.incr_http_queries(store)
        {params, _} = Rocket.Request.query_params(req)

        case extract_query_params(params) do
          {:ok, metric, labels, from, to} ->
            {:ok, results} =
              TimelessMetrics.query_multi(store, metric, labels, from: from, to: to)

            body =
              results
              |> Enum.map(fn %{labels: l, points: pts} ->
                {timestamps, values} = Enum.unzip(pts)

                json_encode!(%{
                  metric: Map.put(l, "__name__", metric),
                  values: values,
                  timestamps: Enum.map(timestamps, &(&1 * 1000))
                })
              end)
              |> Enum.join("\n")

            Rocket.Response.send_iodata(req, 200, [{"content-type", "application/json"}], body)

          {:error, msg} ->
            json_error(req, 400, msg)
        end
    end
  end

  # Latest value for matching series
  get "/api/v1/query" do
    handle_api_query(req)
  end

  post "/api/v1/query" do
    handle_api_query(req)
  end

  defp handle_api_query(req) do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        TimelessMetrics.Stats.incr_http_queries(store)
        params = merged_params(req)

        if params["query"] do
          # PromQL instant query — evaluate at `time` (default: now)
          now = System.os_time(:second)
          eval_time = parse_prom_time(params["time"], now)
          lookback = 300

          case TimelessMetrics.PromQL.parse(params["query"]) do
            {:ok, plan} ->
              case TimelessMetrics.PromQL.execute(
                     plan,
                     store,
                     eval_time - lookback,
                     eval_time,
                     lookback
                   ) do
                {:ok, response} ->
                  # Convert range response to instant: keep only the last point per series
                  instant_response = to_instant_response(response, eval_time)
                  json_resp(req, 200, instant_response)

                {:error, reason} ->
                  promql_error(req, store, params["query"], :execution, reason)
              end

            {:error, reason} ->
              promql_error(req, store, params["query"], :parse, reason)
          end
        else
          case extract_metric_and_labels(params) do
            {:ok, metric, labels} ->
              {:ok, results} = TimelessMetrics.query_multi(store, metric, labels)

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
                  [single] -> single
                  multiple -> %{data: multiple}
                end

              json_resp(req, 200, body)

            {:error, msg} ->
              json_error(req, 400, msg)
          end
        end
    end
  end

  # Range query with bucketed aggregation (multi-series)
  # When query= param is present, routes through PromQL parser (TSBS/Grafana compatible).
  # Otherwise uses native params: metric=, metrics=, group_by=, cross_aggregate=, etc.
  get "/api/v1/query_range" do
    handle_api_query_range(req)
  end

  post "/api/v1/query_range" do
    handle_api_query_range(req)
  end

  defp handle_api_query_range(req) do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        TimelessMetrics.Stats.incr_http_queries(store)
        params = merged_params(req)

        # If query= param is present, treat as PromQL (TSBS sends PromQL here)
        if params["query"] do
          now = System.os_time(:second)
          start_ts = parse_prom_time(params["start"], now - 3600)
          end_ts = parse_prom_time(params["end"], now)
          step = parse_prom_step(params["step"], 60)

          case TimelessMetrics.PromQL.parse(params["query"]) do
            {:ok, plan} ->
              case TimelessMetrics.PromQL.execute(plan, store, start_ts, end_ts, step) do
                {:ok, response} ->
                  json_resp(req, 200, response)

                {:error, reason} ->
                  promql_error(req, store, params["query"], :execution, reason)
              end

            {:error, reason} ->
              promql_error(req, store, params["query"], :parse, reason)
          end
        else
          case extract_query_params_extended(params) do
            {:ok, query_spec} ->
              step = parse_int(params["step"], 60)
              agg = parse_aggregate(params["aggregate"])
              transform = TimelessMetrics.Transform.parse(params["transform"])
              group_by = params["group_by"]
              cross_agg = parse_aggregate_or_nil(params["cross_aggregate"])
              threshold = parse_threshold_params(params)
              limit = parse_int_or_nil(params["limit"])

              base_opts = [
                from: query_spec.from,
                to: query_spec.to,
                bucket: {step, :seconds},
                aggregate: agg,
                transform: transform
              ]

              {result_type, results} =
                case {query_spec.metrics, group_by} do
                  {metrics, group_by} when is_list(metrics) and is_binary(group_by) ->
                    group_keys =
                      String.split(group_by, ",", trim: true) |> Enum.map(&String.trim/1)

                    {:ok, grouped} =
                      TimelessMetrics.query_aggregate_grouped_metrics(
                        store,
                        metrics,
                        query_spec.labels,
                        Keyword.merge(
                          base_opts,
                          [group_by: group_keys] ++
                            if(cross_agg, do: [cross_series_aggregate: cross_agg], else: [])
                        )
                      )

                    {:grouped, grouped}

                  {metrics, _} when is_list(metrics) ->
                    {:ok, multi} =
                      TimelessMetrics.query_aggregate_multi_metrics(
                        store,
                        metrics,
                        query_spec.labels,
                        base_opts
                      )

                    {:multi, multi}

                  {_, group_by} when is_binary(group_by) ->
                    group_keys =
                      String.split(group_by, ",", trim: true) |> Enum.map(&String.trim/1)

                    {:ok, grouped} =
                      TimelessMetrics.query_aggregate_grouped(
                        store,
                        query_spec.metric,
                        query_spec.labels,
                        Keyword.merge(
                          base_opts,
                          [group_by: group_keys] ++
                            if(cross_agg, do: [cross_series_aggregate: cross_agg], else: [])
                        )
                      )

                    {:grouped, grouped}

                  _ when threshold != nil ->
                    {:ok, filtered} =
                      TimelessMetrics.query_aggregate_multi_filtered(
                        store,
                        query_spec.metric,
                        query_spec.labels,
                        Keyword.put(base_opts, :threshold, threshold)
                      )

                    {:flat, filtered}

                  _ ->
                    {:ok, flat} =
                      TimelessMetrics.query_aggregate_multi(
                        store,
                        query_spec.metric,
                        query_spec.labels,
                        base_opts
                      )

                    {:flat, flat}
                end

              results = maybe_apply_limit(results, limit)
              body = format_native_response(result_type, results, query_spec)
              json_resp(req, 200, body)

            {:error, msg} ->
              json_error(req, 400, msg)
          end
        end
    end
  end

  # List all label names (VictoriaMetrics native path)
  get "/api/v1/labels" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {:ok, metrics} = TimelessMetrics.list_metrics(store)

        label_names =
          metrics
          |> Enum.flat_map(fn metric ->
            case TimelessMetrics.list_series(store, metric) do
              {:ok, series} -> Enum.flat_map(series, fn %{labels: l} -> Map.keys(l) end)
              _ -> []
            end
          end)
          |> MapSet.new()
          |> MapSet.put("__name__")
          |> MapSet.to_list()
          |> Enum.sort()

        json_resp(req, 200, %{"status" => "success", "data" => label_names})
    end
  end

  # List all metric names
  get "/api/v1/label/__name__/values" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {:ok, metrics} = TimelessMetrics.list_metrics(store)
        json_resp(req, 200, %{status: "success", data: metrics})
    end
  end

  # List values for a specific label key
  # When metric= is provided, scopes to that metric. Otherwise queries all metrics (VM compat).
  get "/api/v1/label/:name/values" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {params, _} = Rocket.Request.query_params(req)
        label_name = req.path_params["name"]
        metric = params["metric"]

        values =
          if metric do
            {:ok, vals} = TimelessMetrics.label_values(store, metric, label_name)
            vals
          else
            {:ok, metrics} = TimelessMetrics.list_metrics(store)

            metrics
            |> Enum.flat_map(fn m ->
              case TimelessMetrics.label_values(store, m, label_name) do
                {:ok, vals} -> vals
                _ -> []
              end
            end)
            |> Enum.uniq()
            |> Enum.sort()
          end

        json_resp(req, 200, %{status: "success", data: values})
    end
  end

  # List all series for a metric
  get "/api/v1/series" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {params, _} = Rocket.Request.query_params(req)

        case params["metric"] do
          nil ->
            json_error(req, 400, "missing required parameter: metric")

          metric ->
            {:ok, series} = TimelessMetrics.list_series(store, metric)
            json_resp(req, 200, %{status: "success", data: series})
        end
    end
  end

  # Register or update metric metadata
  post "/api/v1/metadata" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        body = req.body

        case safe_json_decode(body) do
          %{"metric" => metric, "type" => type} = params
          when type in ~w(gauge counter histogram) ->
            TimelessMetrics.register_metric(store, metric, String.to_existing_atom(type),
              unit: params["unit"],
              description: params["description"]
            )

            json_resp(req, 200, %{status: "ok"})

          %{"metric" => _} ->
            json_error(req, 400, "type must be one of: gauge, counter, histogram")

          _ ->
            json_error(req, 400, "invalid JSON: requires metric and type fields")
        end
    end
  end

  # Get metric metadata
  get "/api/v1/metadata" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {params, _} = Rocket.Request.query_params(req)

        case params["metric"] do
          nil ->
            json_error(req, 400, "missing required parameter: metric")

          metric ->
            {:ok, meta} = TimelessMetrics.get_metadata(store, metric)

            if meta do
              json_resp(req, 200, %{
                metric: metric,
                type: meta.type,
                unit: meta.unit,
                description: meta.description
              })
            else
              json_resp(req, 200, %{
                metric: metric,
                type: "gauge",
                unit: nil,
                description: nil
              })
            end
        end
    end
  end

  # Create an annotation
  post "/api/v1/annotations" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        body = req.body

        case safe_json_decode(body) do
          %{"title" => title} = params ->
            timestamp = params["timestamp"] || System.os_time(:second)
            tags = params["tags"] || []
            description = params["description"]

            {:ok, id} =
              TimelessMetrics.annotate(store, timestamp, title,
                description: description,
                tags: tags
              )

            json_resp(req, 201, %{id: id, status: "created"})

          _ ->
            json_error(req, 400, "invalid JSON: requires title field")
        end
    end
  end

  # Query annotations in a time range
  get "/api/v1/annotations" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {params, _} = Rocket.Request.query_params(req)

        now = System.os_time(:second)
        from = parse_time(params["from"], now - 86_400)
        to = parse_time(params["to"], now)

        tag_filter =
          case params["tags"] do
            nil -> []
            tags_str -> String.split(tags_str, ",", trim: true)
          end

        {:ok, results} = TimelessMetrics.annotations(store, from, to, tags: tag_filter)
        json_resp(req, 200, %{data: results})
    end
  end

  # Delete an annotation
  delete "/api/v1/annotations/:id" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {id, _} = Integer.parse(req.path_params["id"])
        TimelessMetrics.delete_annotation(store, id)
        json_resp(req, 200, %{status: "deleted"})
    end
  end

  # Create an alert rule
  post "/api/v1/alerts" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        body = req.body

        case safe_json_decode(body) do
          %{
            "name" => name,
            "metric" => metric,
            "condition" => cond_str,
            "threshold" => threshold
          } = params
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

            {:ok, id} = TimelessMetrics.create_alert(store, opts)
            json_resp(req, 201, %{id: id, status: "created"})

          _ ->
            json_error(
              req,
              400,
              "requires: name, metric, condition (above/below), threshold (number)"
            )
        end
    end
  end

  # List all alert rules with state
  get "/api/v1/alerts" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {:ok, rules} = TimelessMetrics.list_alerts(store)
        json_resp(req, 200, %{data: rules})
    end
  end

  # Delete an alert rule
  delete "/api/v1/alerts/:id" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {id, _} = Integer.parse(req.path_params["id"])
        TimelessMetrics.delete_alert(store, id)
        json_resp(req, 200, %{status: "deleted"})
    end
  end

  # Forecast future values
  get "/api/v1/forecast" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {params, _} = Rocket.Request.query_params(req)

        case extract_query_params(params) do
          {:ok, metric, labels, from, to} ->
            step = parse_int(params["step"], 300)
            horizon = parse_duration_param(params["horizon"], 3600)
            transform = TimelessMetrics.Transform.parse(params["transform"])

            {:ok, results} =
              TimelessMetrics.query_aggregate_multi(store, metric, labels,
                from: from,
                to: to,
                bucket: {step, :seconds},
                aggregate: :avg,
                transform: transform
              )

            forecasts =
              Enum.map(results, fn %{labels: l, data: data} ->
                case TimelessMetrics.Forecast.predict(data, horizon: horizon, bucket: step) do
                  {:ok, predictions} ->
                    %{
                      labels: l,
                      data: Enum.map(data, fn {ts, val} -> [ts, val] end),
                      forecast: Enum.map(predictions, fn {ts, val} -> [ts, val] end)
                    }

                  {:error, _} ->
                    %{
                      labels: l,
                      data: Enum.map(data, fn {ts, val} -> [ts, val] end),
                      forecast: []
                    }
                end
              end)

            json_resp(req, 200, %{metric: metric, series: forecasts})

          {:error, msg} ->
            json_error(req, 400, msg)
        end
    end
  end

  # Anomaly detection
  get "/api/v1/anomalies" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {params, _} = Rocket.Request.query_params(req)

        case extract_query_params(params) do
          {:ok, metric, labels, from, to} ->
            step = parse_int(params["step"], 300)
            sensitivity = parse_sensitivity(params["sensitivity"])
            transform = TimelessMetrics.Transform.parse(params["transform"])

            {:ok, results} =
              TimelessMetrics.query_aggregate_multi(store, metric, labels,
                from: from,
                to: to,
                bucket: {step, :seconds},
                aggregate: :avg,
                transform: transform
              )

            detections =
              Enum.map(results, fn %{labels: l, data: data} ->
                case TimelessMetrics.Anomaly.detect(data, sensitivity: sensitivity) do
                  {:ok, analysis} -> %{labels: l, analysis: analysis}
                  {:error, _} -> %{labels: l, analysis: []}
                end
              end)

            json_resp(req, 200, %{metric: metric, series: detections})

          {:error, msg} ->
            json_error(req, 400, msg)
        end
    end
  end

  # SVG chart — embeddable via <img src="http://host:port/chart?metric=cpu&host=web-1&from=-1h">
  # Optional: &forecast=1h for forecast overlay, &anomalies=medium for anomaly markers
  get "/chart" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {params, _} = Rocket.Request.query_params(req)

        case extract_chart_params(params) do
          {:ok, metric, labels, from, to, step, agg, width, height, theme} ->
            transform = TimelessMetrics.Transform.parse(params["transform"])

            {:ok, results} =
              TimelessMetrics.query_aggregate_multi(store, metric, labels,
                from: from,
                to: to,
                bucket: {step, :seconds},
                aggregate: agg,
                transform: transform
              )

            {:ok, annots} = TimelessMetrics.annotations(store, from, to)

            # Optional forecast overlay
            forecast_data =
              case params["forecast"] do
                nil ->
                  []

                horizon_str ->
                  horizon = parse_duration_param(horizon_str, 3600)

                  case results do
                    [%{data: data} | _] ->
                      case TimelessMetrics.Forecast.predict(data, horizon: horizon, bucket: step) do
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
                    case TimelessMetrics.Anomaly.detect(data, sensitivity: sensitivity) do
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
              TimelessMetrics.Chart.render(metric, results,
                width: width,
                height: height,
                theme: theme,
                x_domain: {from, to},
                annotations: annots,
                forecast: forecast_data,
                anomalies: anomaly_points
              )

            Rocket.Response.send_iodata(
              req,
              200,
              [{"content-type", "image/svg+xml"}, {"cache-control", "public, max-age=60"}],
              svg
            )

          {:error, msg} ->
            json_error(req, 400, msg)
        end
    end
  end

  # Prometheus text exposition format import
  # Each line: metric_name{label1="val1",label2="val2"} value [timestamp_ms]
  post "/api/v1/import/prometheus" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        TimelessMetrics.Stats.incr_http_imports(store)
        body = req.body

        # Queue for background processing — return 204 immediately
        queue = :persistent_term.get({TimelessMetrics, store, :ingest_queue})
        TimelessMetrics.IngestWorker.enqueue(queue, body, :prometheus)
        send_resp(req, 204)
    end
  end

  # Prometheus-compatible query_range endpoint (for Grafana + TSBS)
  get "/prometheus/api/v1/query_range" do
    handle_prom_query_range(req)
  end

  post "/prometheus/api/v1/query_range" do
    handle_prom_query_range(req)
  end

  defp handle_prom_query_range(req) do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        TimelessMetrics.Stats.incr_http_queries(store)
        params = merged_params(req)

        case params["query"] do
          nil ->
            prom_error(req, 400, "bad_data", "missing required parameter: query")

          query ->
            now = System.os_time(:second)
            start_ts = parse_prom_time(params["start"], now - 3600)
            end_ts = parse_prom_time(params["end"], now)
            step = parse_prom_step(params["step"], 60)

            case TimelessMetrics.PromQL.parse(query) do
              {:ok, plan} ->
                case TimelessMetrics.PromQL.execute(plan, store, start_ts, end_ts, step) do
                  {:ok, response} ->
                    json_resp(req, 200, response)

                  {:error, reason} ->
                    promql_error(req, store, params["query"], :execution, reason)
                end

              {:error, reason} ->
                promql_error(req, store, params["query"], :parse, reason)
            end
        end
    end
  end

  # Prometheus-compatible instant query endpoint (for Grafana health check + current-value panels)
  get "/prometheus/api/v1/query" do
    handle_prom_query(req)
  end

  post "/prometheus/api/v1/query" do
    handle_prom_query(req)
  end

  defp handle_prom_query(req) do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        TimelessMetrics.Stats.incr_http_queries(store)
        params = merged_params(req)

        case params["query"] do
          nil ->
            prom_error(req, 400, "bad_data", "missing required parameter: query")

          query ->
            now = System.os_time(:second)
            time = parse_prom_time(params["time"], now)
            start_ts = time - 300
            end_ts = time
            step = 300

            case TimelessMetrics.PromQL.parse(query) do
              {:ok, plan} ->
                case TimelessMetrics.PromQL.execute(plan, store, start_ts, end_ts, step) do
                  {:ok, response} ->
                    # Convert matrix results to vector (take last value from each series)
                    vector_results =
                      Enum.map(response["data"]["result"], fn series ->
                        case List.last(series["values"]) do
                          [ts, val] -> %{"metric" => series["metric"], "value" => [ts, val]}
                          _ -> %{"metric" => series["metric"], "value" => [end_ts, "0"]}
                        end
                      end)

                    vector_response = %{
                      "status" => "success",
                      "data" => %{"resultType" => "vector", "result" => vector_results}
                    }

                    json_resp(req, 200, vector_response)

                  {:error, reason} ->
                    promql_error(req, store, params["query"], :execution, reason)
                end

              {:error, reason} ->
                promql_error(req, store, params["query"], :parse, reason)
            end
        end
    end
  end

  # Prometheus-compatible labels endpoint (for Grafana label autocomplete)
  get "/prometheus/api/v1/labels" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {:ok, metrics} = TimelessMetrics.list_metrics(store)

        label_names =
          metrics
          |> Enum.flat_map(fn metric ->
            case TimelessMetrics.list_series(store, metric) do
              {:ok, series} -> Enum.flat_map(series, fn %{labels: l} -> Map.keys(l) end)
              _ -> []
            end
          end)
          |> MapSet.new()
          |> MapSet.put("__name__")
          |> MapSet.to_list()
          |> Enum.sort()

        json_resp(req, 200, %{"status" => "success", "data" => label_names})
    end
  end

  # Prometheus-compatible label values endpoint (no metric= param required)
  get "/prometheus/api/v1/label/:name/values" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        label_name = req.path_params["name"]

        values =
          if label_name == "__name__" do
            {:ok, metrics} = TimelessMetrics.list_metrics(store)
            metrics
          else
            {:ok, metrics} = TimelessMetrics.list_metrics(store)

            metrics
            |> Enum.flat_map(fn metric ->
              case TimelessMetrics.label_values(store, metric, label_name) do
                {:ok, vals} -> vals
                _ -> []
              end
            end)
            |> Enum.uniq()
            |> Enum.sort()
          end

        json_resp(req, 200, %{"status" => "success", "data" => values})
    end
  end

  # Prometheus-compatible series endpoint (accepts match[] param)
  get "/prometheus/api/v1/series" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {params, _} = Rocket.Request.query_params(req)

        match_param =
          case params["match[]"] || params["match"] do
            [first | _] -> first
            other -> other
          end

        case match_param do
          nil ->
            json_error(req, 400, "missing required parameter: match[]")

          match_query ->
            case TimelessMetrics.PromQL.parse(match_query) do
              {:ok, ast} ->
                plan = TimelessMetrics.PromQL.selector_info(ast)

                metric_names =
                  case {plan.metric, plan.metric_pattern} do
                    {name, nil} when is_binary(name) ->
                      [name]

                    {nil, pattern} when is_binary(pattern) ->
                      {:ok, all_metrics} = TimelessMetrics.list_metrics(store)
                      {:ok, regex} = Regex.compile("^(?:" <> pattern <> ")$")
                      Enum.filter(all_metrics, &Regex.match?(regex, &1))

                    _ ->
                      {:ok, all_metrics} = TimelessMetrics.list_metrics(store)
                      all_metrics
                  end

                series =
                  Enum.flat_map(metric_names, fn metric ->
                    case TimelessMetrics.list_series(store, metric) do
                      {:ok, series_list} ->
                        label_maps = Enum.map(series_list, fn %{labels: l} -> l end)

                        label_maps
                        |> filter_series_by_labels(plan.labels)
                        |> Enum.map(&Map.put(&1, "__name__", metric))

                      _ ->
                        []
                    end
                  end)

                json_resp(req, 200, %{"status" => "success", "data" => series})

              {:error, reason} ->
                json_error(req, 400, "PromQL parse error: #{reason}")
            end
        end
    end
  end

  # Dashboard — zero-dependency HTML overview page
  get "/" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        {params, _} = Rocket.Request.query_params(req)

        from = params["from"] || "-1h"
        to = params["to"] || "now"
        filter = label_params(params)

        html =
          TimelessMetrics.Dashboard.render(
            store: store,
            from: from,
            to: to,
            filter: filter
          )

        html_resp(req, 200, html)
    end
  end

  # --- OpenAPI / API Docs ---

  # Serve the OpenAPI JSON spec
  get "/api/openapi.json" do
    Rocket.Response.send_iodata(
      req,
      200,
      [{"content-type", "application/json"}, {"access-control-allow-origin", "*"}],
      TimelessMetrics.OpenAPI.spec_json()
    )
  end

  # Serve the Scalar API reference UI
  get "/api/docs" do
    html_resp(req, 200, scalar_html())
  end

  # --- Scrape Target CRUD ---

  # Create a scrape target
  post "/api/v1/scrape_targets" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        scraper = :"#{store}_scraper"
        body = req.body

        case safe_json_decode(body) do
          params when is_map(params) ->
            case TimelessMetrics.Scraper.add_target(scraper, params) do
              {:ok, id} ->
                json_resp(req, 201, %{id: id, status: "created"})

              {:error, reason} ->
                json_error(req, 400, to_string(reason))
            end

          _ ->
            json_error(req, 400, "invalid JSON")
        end
    end
  end

  # List all scrape targets with health
  get "/api/v1/scrape_targets" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        scraper = :"#{store}_scraper"
        {:ok, targets} = TimelessMetrics.Scraper.list_targets(scraper)
        json_resp(req, 200, %{data: targets})
    end
  end

  # Get a single scrape target with health
  get "/api/v1/scrape_targets/:id" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        scraper = :"#{store}_scraper"
        {target_id, _} = Integer.parse(req.path_params["id"])

        case TimelessMetrics.Scraper.get_target(scraper, target_id) do
          {:ok, target} ->
            json_resp(req, 200, target)

          {:error, :not_found} ->
            json_error(req, 404, "target not found")
        end
    end
  end

  # Update a scrape target
  put "/api/v1/scrape_targets/:id" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        scraper = :"#{store}_scraper"
        {target_id, _} = Integer.parse(req.path_params["id"])
        body = req.body

        case safe_json_decode(body) do
          params when is_map(params) ->
            case TimelessMetrics.Scraper.update_target(scraper, target_id, params) do
              :ok ->
                json_resp(req, 200, %{status: "updated"})

              {:error, reason} ->
                json_error(req, 400, to_string(reason))
            end

          _ ->
            json_error(req, 400, "invalid JSON")
        end
    end
  end

  # Delete a scrape target
  delete "/api/v1/scrape_targets/:id" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        scraper = :"#{store}_scraper"
        {target_id, _} = Integer.parse(req.path_params["id"])
        :ok = TimelessMetrics.Scraper.delete_target(scraper, target_id)
        json_resp(req, 200, %{status: "deleted"})
    end
  end

  # Prometheus exposition format endpoint for self-scraping
  get "/metrics" do
    case check_auth(req) do
      :halt ->
        :ok

      :ok ->
        store = store()
        info = TimelessMetrics.info(store)
        mem = :erlang.memory()
        {:ok, hostname} = :inet.gethostname()
        host = to_string(hostname)
        cpu_rq = :erlang.statistics(:total_run_queue_lengths)
        all_rq = :erlang.statistics(:total_run_queue_lengths_all)
        io_rq = all_rq - cpu_rq

        lines = [
          "# HELP vm_memory_total_bytes Total BEAM memory usage in bytes.",
          "# TYPE vm_memory_total_bytes gauge",
          ~s(vm_memory_total_bytes{host="#{host}"} #{mem[:total]}),
          "# HELP vm_memory_processes_bytes BEAM process memory in bytes.",
          "# TYPE vm_memory_processes_bytes gauge",
          ~s(vm_memory_processes_bytes{host="#{host}"} #{mem[:processes]}),
          "# HELP vm_memory_ets_bytes BEAM ETS table memory in bytes.",
          "# TYPE vm_memory_ets_bytes gauge",
          ~s(vm_memory_ets_bytes{host="#{host}"} #{mem[:ets]}),
          "# HELP vm_memory_binary_bytes BEAM binary memory in bytes.",
          "# TYPE vm_memory_binary_bytes gauge",
          ~s(vm_memory_binary_bytes{host="#{host}"} #{mem[:binary]}),
          "# HELP vm_memory_atom_bytes BEAM atom memory in bytes.",
          "# TYPE vm_memory_atom_bytes gauge",
          ~s(vm_memory_atom_bytes{host="#{host}"} #{mem[:atom]}),
          "# HELP vm_memory_system_bytes BEAM system memory in bytes.",
          "# TYPE vm_memory_system_bytes gauge",
          ~s(vm_memory_system_bytes{host="#{host}"} #{mem[:system]}),
          "# HELP vm_process_count Number of BEAM processes.",
          "# TYPE vm_process_count gauge",
          ~s(vm_process_count{host="#{host}"} #{:erlang.system_info(:process_count)}),
          "# HELP vm_process_limit Maximum number of BEAM processes.",
          "# TYPE vm_process_limit gauge",
          ~s(vm_process_limit{host="#{host}"} #{:erlang.system_info(:process_limit)}),
          "# HELP vm_port_count Number of BEAM ports.",
          "# TYPE vm_port_count gauge",
          ~s(vm_port_count{host="#{host}"} #{:erlang.system_info(:port_count)}),
          "# HELP vm_port_limit Maximum number of BEAM ports.",
          "# TYPE vm_port_limit gauge",
          ~s(vm_port_limit{host="#{host}"} #{:erlang.system_info(:port_limit)}),
          "# HELP vm_atom_count Number of atoms.",
          "# TYPE vm_atom_count gauge",
          ~s(vm_atom_count{host="#{host}"} #{:erlang.system_info(:atom_count)}),
          "# HELP vm_atom_limit Maximum number of atoms.",
          "# TYPE vm_atom_limit gauge",
          ~s(vm_atom_limit{host="#{host}"} #{:erlang.system_info(:atom_limit)}),
          "# HELP vm_run_queue_total Total BEAM scheduler run queue length.",
          "# TYPE vm_run_queue_total gauge",
          ~s(vm_run_queue_total{host="#{host}"} #{all_rq}),
          "# HELP vm_run_queue_cpu CPU scheduler run queue length.",
          "# TYPE vm_run_queue_cpu gauge",
          ~s(vm_run_queue_cpu{host="#{host}"} #{cpu_rq}),
          "# HELP vm_run_queue_io IO scheduler run queue length.",
          "# TYPE vm_run_queue_io gauge",
          ~s(vm_run_queue_io{host="#{host}"} #{io_rq}),
          "# HELP timeless_series_count Number of active metric series.",
          "# TYPE timeless_series_count gauge",
          ~s(timeless_series_count{host="#{host}"} #{info.series_count}),
          "# HELP timeless_total_points Total stored data points.",
          "# TYPE timeless_total_points gauge",
          ~s(timeless_total_points{host="#{host}"} #{info.total_points}),
          "# HELP timeless_storage_bytes Storage size in bytes.",
          "# TYPE timeless_storage_bytes gauge",
          ~s(timeless_storage_bytes{host="#{host}"} #{info.storage_bytes}),
          "# HELP timeless_buffer_points Number of points in raw buffer.",
          "# TYPE timeless_buffer_points gauge",
          ~s(timeless_buffer_points{host="#{host}"} #{info.raw_buffer_points})
        ]

        text_resp(req, 200, Enum.join(lines, "\n") <> "\n")
    end
  end

  match _ do
    send_resp(req, 404, "not found")
  end

  # --- Internals ---

  @reserved_params ~w(metric metrics from to start end step aggregate width height label_key theme transform token forecast anomalies sensitivity horizon group_by cross_aggregate threshold_gt threshold_lt limit)

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

  defp extract_query_params_extended(params) do
    now = System.os_time(:second)
    from = parse_time(params["start"], parse_time(params["from"], now - 3600))
    to = parse_time(params["end"], parse_time(params["to"], now))

    # Support =~ prefix for regex labels
    labels = label_params_extended(params)

    case {params["metric"], params["metrics"]} do
      {nil, nil} ->
        {:error, "missing required parameter: metric or metrics"}

      {metric, nil} ->
        {:ok, %{metric: metric, metrics: nil, labels: labels, from: from, to: to}}

      {_, metrics_str} ->
        metrics = String.split(metrics_str, ",", trim: true) |> Enum.map(&String.trim/1)
        {:ok, %{metric: nil, metrics: metrics, labels: labels, from: from, to: to}}
    end
  end

  defp label_params(params) do
    params
    |> Map.drop(@reserved_params)
    |> Map.new(fn {k, v} -> {to_string(k), to_string(v)} end)
  end

  defp label_params_extended(params) do
    params
    |> Map.drop(@reserved_params)
    |> Map.new(fn {k, v} ->
      v = to_string(v)

      if String.starts_with?(v, "=~") do
        {to_string(k), {:regex, String.trim_leading(v, "=~")}}
      else
        {to_string(k), v}
      end
    end)
  end

  defp parse_aggregate_or_nil(nil), do: nil

  defp parse_aggregate_or_nil(agg) when agg in ~w(avg min max sum count),
    do: String.to_existing_atom(agg)

  defp parse_aggregate_or_nil(_), do: nil

  defp parse_threshold_params(params) do
    cond do
      params["threshold_gt"] ->
        case Float.parse(params["threshold_gt"]) do
          {n, _} -> {:gt, n}
          :error -> nil
        end

      params["threshold_lt"] ->
        case Float.parse(params["threshold_lt"]) do
          {n, _} -> {:lt, n}
          :error -> nil
        end

      true ->
        nil
    end
  end

  defp parse_int_or_nil(nil), do: nil

  defp parse_int_or_nil(val) do
    case Integer.parse(val) do
      {n, _} -> n
      :error -> nil
    end
  end

  defp maybe_apply_limit(results, nil), do: results
  defp maybe_apply_limit(results, limit), do: TimelessMetrics.top_n(results, limit)

  defp format_native_response(:flat, results, query_spec) do
    series =
      Enum.map(results, fn %{labels: l, data: buckets} ->
        %{labels: l, data: Enum.map(buckets, fn {ts, val} -> [ts, val] end)}
      end)

    %{metric: query_spec.metric, series: series}
  end

  defp format_native_response(:multi, results, _query_spec) do
    series =
      Enum.map(results, fn result ->
        %{
          metric: result.metric,
          labels: result.labels,
          data: Enum.map(result.data, fn {ts, val} -> [ts, val] end)
        }
      end)

    %{series: series}
  end

  defp format_native_response(:grouped, results, query_spec) do
    groups =
      Enum.map(results, fn %{group: g, data: data} ->
        %{group: g, data: Enum.map(data, fn {ts, val} -> [ts, val] end)}
      end)

    %{metric: query_spec.metric || query_spec.metrics, groups: groups}
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

  defp filter_series_by_labels(series_list, labels) when map_size(labels) == 0, do: series_list

  defp filter_series_by_labels(series_list, labels) do
    Enum.filter(series_list, fn series_labels ->
      Enum.all?(labels, fn
        {key, {:regex, pattern}} ->
          case Map.get(series_labels, key) do
            nil ->
              false

            val ->
              {:ok, regex} = Regex.compile("^(?:" <> pattern <> ")$")
              Regex.match?(regex, val)
          end

        {key, {:not_regex, pattern}} ->
          case Map.get(series_labels, key) do
            nil ->
              true

            val ->
              {:ok, regex} = Regex.compile("^(?:" <> pattern <> ")$")
              not Regex.match?(regex, val)
          end

        {key, {:not_equal, value}} ->
          Map.get(series_labels, key) != value

        {key, value} when is_binary(value) ->
          Map.get(series_labels, key) == value
      end)
    end)
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
    db_path = TimelessMetrics.DB.db_path(:"#{store}_db")
    data_dir = Path.dirname(db_path)
    Path.join([data_dir, "backups", to_string(System.os_time(:second))])
  end

  defp parse_theme("dark"), do: :dark
  defp parse_theme("light"), do: :light
  defp parse_theme(_), do: :auto

  defp json_encode!(term), do: term |> nullify() |> :json.encode() |> IO.iodata_to_binary()

  defp nullify(nil), do: :null
  defp nullify(map) when is_map(map), do: Map.new(map, fn {k, v} -> {k, nullify(v)} end)
  defp nullify(list) when is_list(list), do: Enum.map(list, &nullify/1)
  defp nullify(other), do: other

  defp scalar_html do
    """
    <!doctype html>
    <html>
    <head>
      <title>TimelessMetrics API</title>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
    </head>
    <body>
      <div id="app"></div>
      <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
      <script>
        Scalar.createApiReference('#app', {
          url: '/api/openapi.json',
          theme: 'kepler',
          hideClientButton: false,
          defaultHttpClient: { targetKey: 'shell', clientKey: 'curl' }
        })
      </script>
    </body>
    </html>
    """
  end

  @max_error_samples 3
  @parallel_parse_threshold 2_000

  defp safe_json_decode(bin) do
    :json.decode(bin)
  catch
    :error, _ -> :error
  end

  defp merge_parse_results(results) do
    Enum.reduce(results, {%{}, 0, 0, []}, fn {groups, count, errors, samples},
                                             {acc_groups, acc_count, acc_errors, acc_samples} ->
      merged =
        Enum.reduce(groups, acc_groups, fn {key, points}, acc ->
          Map.update(acc, key, points, &(points ++ &1))
        end)

      {merged, acc_count + count, acc_errors + errors, acc_samples ++ samples}
    end)
  end

  # --- InfluxDB line protocol parser ---
  # Format: measurement,tag=val,tag=val field=value[,field=value] [timestamp_ns]
  # TSBS uses this format for data loading via POST /write.
  # Only numeric field values are ingested (string/bool fields skipped).
  # Multi-field lines produce one entry per numeric field, using field name
  # as part of the metric name: "measurement_fieldname".

  defp ingest_influx_lines(store, body) do
    lines = :binary.split(body, <<"\n">>, [:global])

    {all_entries, errors, error_samples} =
      if length(lines) >= @parallel_parse_threshold do
        parse_influx_lines_parallel(lines)
      else
        parse_influx_lines_sequential(lines)
      end

    if all_entries != [] do
      TimelessMetrics.write_batch(store, all_entries)
    end

    {length(all_entries), errors, error_samples}
  end

  defp parse_influx_lines_sequential(lines) do
    now = System.os_time(:second)

    {entries, errors, samples} =
      Enum.reduce(lines, {[], 0, []}, fn line, {entries_acc, errors, samples} ->
        line = String.trim(line)

        if line == "" or String.starts_with?(line, "#") do
          {entries_acc, errors, samples}
        else
          case parse_influx_line(line, now) do
            {:ok, parsed_entries} ->
              {:lists.reverse(parsed_entries, entries_acc), errors, samples}

            :error ->
              samples =
                if errors < @max_error_samples do
                  [String.slice(line, 0, 200) | samples]
                else
                  samples
                end

              {entries_acc, errors + 1, samples}
          end
        end
      end)

    {entries, errors, Enum.take(Enum.reverse(samples), @max_error_samples)}
  end

  defp parse_influx_lines_parallel(lines) do
    chunk_count = System.schedulers_online()

    chunks =
      lines
      |> Enum.chunk_every(div(length(lines), chunk_count) + 1)
      |> Enum.map(fn chunk ->
        Task.async(fn -> parse_influx_lines_sequential(chunk) end)
      end)

    results = Task.await_many(chunks, :timer.seconds(30))
    merge_parse_results(results)
  end

  # Parse a single InfluxDB line protocol line.
  # Returns {:ok, [{metric, labels, value, timestamp}, ...]} or :error.
  #
  # Line structure: measurement[,tag=val...] field=val[,field=val...] [timestamp_ns]
  # The tricky part: spaces delimit sections, but tags use commas (no spaces).
  defp parse_influx_line(line, now) do
    # Split into: measurement+tags, fields, optional timestamp
    # First space separates measurement+tags from fields
    case :binary.split(line, <<" ">>) do
      [measurement_tags, fields_and_ts] ->
        # Second space (if present) separates fields from timestamp
        {fields_str, timestamp} =
          case :binary.split(fields_and_ts, <<" ">>) do
            [fields, ts_str] ->
              ts_str = String.trim(ts_str)

              case Integer.parse(ts_str) do
                {ts_ns, _} -> {fields, nanoseconds_to_seconds(ts_ns)}
                :error -> {fields, now}
              end

            [fields] ->
              {fields, now}
          end

        # Parse measurement,tag=val,tag=val
        {measurement, tags} = parse_influx_measurement_tags(measurement_tags)

        # Parse field=val,field=val — each numeric field becomes a separate entry
        case parse_influx_fields(fields_str) do
          [] ->
            :error

          fields ->
            entries =
              Enum.map(fields, fn {field_name, value} ->
                # TSBS convention: measurement_field (e.g., cpu_usage_user)
                # If only one field named "value", just use measurement name
                metric =
                  if field_name == "value" and length(fields) == 1 do
                    measurement
                  else
                    measurement <> "_" <> field_name
                  end

                {metric, tags, value, timestamp}
              end)

            {:ok, entries}
        end

      _ ->
        :error
    end
  end

  defp parse_influx_measurement_tags(str) do
    case :binary.split(str, <<",">>) do
      [measurement] ->
        {measurement, %{}}

      [measurement, tags_str] ->
        tags =
          tags_str
          |> :binary.split(<<",">>, [:global])
          |> Enum.reduce(%{}, fn pair, acc ->
            case :binary.split(pair, <<"=">>) do
              [k, v] -> Map.put(acc, k, v)
              _ -> acc
            end
          end)

        {measurement, tags}
    end
  end

  defp parse_influx_fields(str) do
    str
    |> :binary.split(<<",">>, [:global])
    |> Enum.flat_map(fn pair ->
      case :binary.split(pair, <<"=">>) do
        [key, value_str] ->
          case parse_influx_field_value(value_str) do
            {:ok, num} -> [{key, num}]
            :skip -> []
          end

        _ ->
          []
      end
    end)
  end

  # Parse a field value — only accept numeric values.
  # InfluxDB suffixes integers with "i" (e.g., 42i), floats are bare.
  # String values are quoted ("foo"), booleans are t/f/true/false — skip these.
  defp parse_influx_field_value(<<?\", _rest::binary>>), do: :skip
  defp parse_influx_field_value("t"), do: :skip
  defp parse_influx_field_value("f"), do: :skip
  defp parse_influx_field_value("true"), do: :skip
  defp parse_influx_field_value("false"), do: :skip
  defp parse_influx_field_value("T"), do: :skip
  defp parse_influx_field_value("F"), do: :skip
  defp parse_influx_field_value("True"), do: :skip
  defp parse_influx_field_value("False"), do: :skip
  defp parse_influx_field_value("TRUE"), do: :skip
  defp parse_influx_field_value("FALSE"), do: :skip

  defp parse_influx_field_value(str) do
    # Strip trailing "i" for InfluxDB integer notation
    str =
      if String.ends_with?(str, "i") do
        :binary.part(str, 0, byte_size(str) - 1)
      else
        str
      end

    case Float.parse(str) do
      {num, _} -> {:ok, num}
      :error -> :skip
    end
  end

  # InfluxDB timestamps are nanoseconds. Convert to seconds.
  # Also handle microseconds (13 digits) and milliseconds (10-digit range).
  defp nanoseconds_to_seconds(ts) when ts > 1_000_000_000_000_000_000 do
    # Nanoseconds (19 digits)
    div(ts, 1_000_000_000)
  end

  defp nanoseconds_to_seconds(ts) when ts > 1_000_000_000_000_000 do
    # Microseconds (16 digits)
    div(ts, 1_000_000)
  end

  defp nanoseconds_to_seconds(ts) when ts > 1_000_000_000_000 do
    # Milliseconds (13 digits)
    div(ts, 1_000)
  end

  defp nanoseconds_to_seconds(ts), do: ts

  # Convert a range (matrix) response to an instant (vector) response
  # by keeping only the last value from each series.
  defp to_instant_response(
         %{"status" => "success", "data" => %{"result" => results}},
         eval_time
       ) do
    vector =
      results
      |> Enum.flat_map(fn %{"metric" => metric, "values" => values} ->
        case List.last(values) do
          [_ts, val] -> [%{"metric" => metric, "value" => [eval_time, val]}]
          _ -> []
        end
      end)

    %{
      "status" => "success",
      "data" => %{
        "resultType" => "vector",
        "result" => vector
      }
    }
  end

  # Parse Prometheus time params — unix timestamps (float or int) or RFC3339.
  # RFC3339 must be tried first: Float.parse("2026-07-24T00:00:00Z") happily
  # returns {2026.0, "-07-..."} and would silently become year-as-seconds.
  defp parse_prom_time(nil, default), do: default

  defp parse_prom_time(val, default) when is_binary(val) do
    case DateTime.from_iso8601(val) do
      {:ok, dt, _offset} ->
        DateTime.to_unix(dt)

      _ ->
        case Float.parse(val) do
          {ts, ""} -> trunc(ts)
          _ -> default
        end
    end
  end

  # Parse Prometheus step — duration string like "60s"/"5m", or seconds (int or float)
  defp parse_prom_step(nil, default), do: default

  defp parse_prom_step(val, default) when is_binary(val) do
    case Integer.parse(val) do
      {n, "s"} when n > 0 ->
        n

      {n, "m"} when n > 0 ->
        n * 60

      {n, "h"} when n > 0 ->
        n * 3600

      {n, "d"} when n > 0 ->
        n * 86_400

      {n, ""} when n > 0 ->
        n

      _ ->
        case Float.parse(val) do
          {f, ""} when f > 0 -> max(trunc(f), 1)
          _ -> default
        end
    end
  end
end
