defmodule TimelessMetrics.PromQL do
  @moduledoc """
  PromQL parser and executor for TSBS DevOps query patterns.

  Parses PromQL expressions used by TSBS's VictoriaMetrics adapter into
  a structured query plan, then routes to the appropriate TimelessMetrics
  query function.

  ## Supported patterns

      # Simple selector
      cpu_usage_user{hostname="host_0"}

      # Regex label matching
      cpu_usage_user{hostname=~"host_0|host_1"}

      # Range vector with function
      max_over_time(cpu_usage_user{hostname="host_0"}[1h])

      # Outer aggregation with group-by
      max(max_over_time(cpu_usage_user{hostname=~"host_0|host_1"}[1h])) by (hostname)

      # Threshold
      max(max_over_time(cpu_usage_user{hostname="host_0"}[1h])) by (hostname) > 90

      # Multi-metric with regex __name__
      max(max_over_time({__name__=~"cpu_.*",hostname=~"host_0"}[1h])) by (hostname)
  """

  defstruct [
    :metric,
    :metric_pattern,
    :labels,
    :outer_aggregate,
    :inner_function,
    :range_duration,
    :group_by,
    :threshold
  ]

  @type t :: %__MODULE__{
          metric: String.t() | nil,
          metric_pattern: String.t() | nil,
          labels: map(),
          outer_aggregate: atom() | nil,
          inner_function: atom() | nil,
          range_duration: non_neg_integer() | nil,
          group_by: [String.t()] | nil,
          threshold: {:gt, float()} | {:lt, float()} | nil
        }

  @doc """
  Parse a PromQL query string into a structured query plan.

  Returns `{:ok, %PromQL{}}` or `{:error, reason}`.
  """
  def parse(query) when is_binary(query) do
    query = String.trim(query)

    if query == "" do
      {:error, "empty query"}
    else
      {query, threshold} = extract_threshold(query)
      {query, group_by} = extract_group_by(query)
      {query, outer_aggregate} = extract_outer_aggregate(query)
      {query, inner_function, range_duration} = extract_inner_function(query)
      {metric, labels} = extract_selector(query)

      # Separate __name__ regex from regular labels
      {metric, metric_pattern, labels} =
        case Map.pop(labels, "__name__") do
          {{:regex, pattern}, rest_labels} ->
            {nil, pattern, rest_labels}

          {exact_name, rest_labels} when is_binary(exact_name) ->
            {exact_name, nil, rest_labels}

          {nil, _} ->
            {metric, nil, labels}
        end

      plan = %__MODULE__{
        metric: metric,
        metric_pattern: metric_pattern,
        labels: labels,
        outer_aggregate: outer_aggregate,
        inner_function: inner_function,
        range_duration: range_duration,
        group_by: group_by,
        threshold: threshold
      }

      {:ok, plan}
    end
  end

  @doc """
  Execute a parsed PromQL plan against a store.

  Returns `{:ok, prom_response}` with Prometheus API formatted results.
  """
  def execute(%__MODULE__{} = plan, store, start_ts, end_ts, step) do
    aggregate = map_inner_function(plan.inner_function)
    bucket = {step, :seconds}

    base_opts = [
      from: start_ts,
      to: end_ts,
      bucket: bucket,
      aggregate: aggregate
    ]

    results = run_query(plan, store, base_opts)
    format_prometheus_response(results, plan)
  end

  defp run_query(%{metric_pattern: pattern} = plan, store, opts) when is_binary(pattern) do
    # Multi-metric: find metrics matching pattern, then query
    {:ok, all_metrics} = TimelessMetrics.list_metrics(store)
    {:ok, regex} = Regex.compile("^(?:" <> pattern <> ")$")
    matching_metrics = Enum.filter(all_metrics, &Regex.match?(regex, &1))

    case {plan.group_by, plan.threshold} do
      {group_by, _} when is_list(group_by) and group_by != [] ->
        {:ok, grouped} =
          TimelessMetrics.query_aggregate_grouped_metrics(
            store,
            matching_metrics,
            plan.labels,
            Keyword.merge(opts,
              group_by: group_by,
              cross_series_aggregate: plan.outer_aggregate || :max
            )
          )

        {:grouped, grouped}

      {_, {:gt, _} = threshold} ->
        # Apply threshold per-metric, then merge
        all_results =
          Enum.flat_map(matching_metrics, fn metric ->
            {:ok, filtered} =
              TimelessMetrics.query_aggregate_multi_filtered(
                store,
                metric,
                plan.labels,
                Keyword.put(opts, :threshold, threshold)
              )

            Enum.map(filtered, fn r -> Map.put(r, :metric, metric) end)
          end)

        {:multi, all_results}

      {_, {:lt, _} = threshold} ->
        all_results =
          Enum.flat_map(matching_metrics, fn metric ->
            {:ok, filtered} =
              TimelessMetrics.query_aggregate_multi_filtered(
                store,
                metric,
                plan.labels,
                Keyword.put(opts, :threshold, threshold)
              )

            Enum.map(filtered, fn r -> Map.put(r, :metric, metric) end)
          end)

        {:multi, all_results}

      _ ->
        {:ok, results} =
          TimelessMetrics.query_aggregate_multi_metrics(
            store,
            matching_metrics,
            plan.labels,
            opts
          )

        {:multi, results}
    end
  end

  defp run_query(%{metric: metric} = plan, store, opts) when is_binary(metric) do
    case {plan.group_by, plan.threshold} do
      {group_by, threshold} when is_list(group_by) and group_by != [] ->
        merged_opts =
          opts
          |> Keyword.merge(
            group_by: group_by,
            cross_series_aggregate: plan.outer_aggregate || :max
          )

        merged_opts =
          case threshold do
            nil -> merged_opts
            _ -> merged_opts
          end

        {:ok, grouped} =
          TimelessMetrics.query_aggregate_grouped(store, metric, plan.labels, merged_opts)

        # Apply threshold post-grouping if present
        grouped =
          case threshold do
            {:gt, val} ->
              Enum.filter(grouped, fn %{data: data} ->
                Enum.any?(data, fn {_ts, v} -> v > val end)
              end)

            {:lt, val} ->
              Enum.filter(grouped, fn %{data: data} ->
                Enum.any?(data, fn {_ts, v} -> v < val end)
              end)

            nil ->
              grouped
          end

        {:grouped, grouped}

      {_, {:gt, _} = threshold} ->
        {:ok, results} =
          TimelessMetrics.query_aggregate_multi_filtered(
            store,
            metric,
            plan.labels,
            Keyword.put(opts, :threshold, threshold)
          )

        {:flat, results}

      {_, {:lt, _} = threshold} ->
        {:ok, results} =
          TimelessMetrics.query_aggregate_multi_filtered(
            store,
            metric,
            plan.labels,
            Keyword.put(opts, :threshold, threshold)
          )

        {:flat, results}

      _ ->
        {:ok, results} =
          TimelessMetrics.query_aggregate_multi(store, metric, plan.labels, opts)

        {:flat, results}
    end
  end

  defp format_prometheus_response({:grouped, groups}, plan) do
    prom_results =
      Enum.map(groups, fn %{group: group_labels, data: data} ->
        metric_map =
          case plan.metric do
            nil -> group_labels
            m -> Map.put(group_labels, "__name__", m)
          end

        %{
          "metric" => metric_map,
          "values" => Enum.map(data, fn {ts, val} -> [ts, format_value(val)] end)
        }
      end)

    {:ok, wrap_prom_response(prom_results)}
  end

  defp format_prometheus_response({:flat, results}, plan) do
    prom_results =
      Enum.map(results, fn result ->
        labels = Map.get(result, :labels, %{})
        metric_name = Map.get(result, :metric, plan.metric)

        metric_map =
          if metric_name, do: Map.put(labels, "__name__", metric_name), else: labels

        %{
          "metric" => metric_map,
          "values" => Enum.map(result.data, fn {ts, val} -> [ts, format_value(val)] end)
        }
      end)

    {:ok, wrap_prom_response(prom_results)}
  end

  defp format_prometheus_response({:multi, results}, plan) do
    format_prometheus_response({:flat, results}, plan)
  end

  defp wrap_prom_response(results) do
    %{
      "status" => "success",
      "data" => %{
        "resultType" => "matrix",
        "result" => results
      }
    }
  end

  defp format_value(val) when is_float(val), do: Float.to_string(val)
  defp format_value(val) when is_integer(val), do: Float.to_string(val / 1)

  # --- Extraction pipeline ---

  # Extract "> 90" or "< 10" from end of query
  defp extract_threshold(query) do
    case Regex.run(~r/^(.+?)\s*(>|<)\s*([\d.]+)\s*$/, query) do
      [_, rest, ">", val] ->
        {n, _} = Float.parse(val)
        {String.trim(rest), {:gt, n}}

      [_, rest, "<", val] ->
        {n, _} = Float.parse(val)
        {String.trim(rest), {:lt, n}}

      _ ->
        {query, nil}
    end
  end

  # Extract "by (hostname)" or "by (hostname, region)"
  defp extract_group_by(query) do
    case Regex.run(~r/^(.+?)\s+by\s*\(([^)]+)\)\s*$/, query) do
      [_, rest, labels_str] ->
        labels = labels_str |> String.split(",") |> Enum.map(&String.trim/1)
        {String.trim(rest), labels}

      _ ->
        {query, nil}
    end
  end

  # Extract outer aggregate: "max(...)" → :max
  defp extract_outer_aggregate(query) do
    case Regex.run(~r/^(avg|min|max|sum|count)\s*\((.+)\)\s*$/, query) do
      [_, agg, inner] ->
        {String.trim(inner), String.to_atom(agg)}

      _ ->
        {query, nil}
    end
  end

  # Extract inner function: "max_over_time(...[1h])" → {:max_over_time, 3600}
  defp extract_inner_function(query) do
    case Regex.run(
           ~r/^(avg_over_time|min_over_time|max_over_time|sum_over_time|count_over_time|rate|irate)\s*\((.+)\[(\d+)([smhd])\]\)\s*$/,
           query
         ) do
      [_, func, inner, n_str, unit] ->
        {n, _} = Integer.parse(n_str)
        duration = duration_to_seconds(n, unit)
        {String.trim(inner), String.to_atom(func), duration}

      _ ->
        {query, nil, nil}
    end
  end

  # Extract selector: "metric{label=value,...}" or just "{label=value,...}"
  defp extract_selector(query) do
    query = String.trim(query)

    case Regex.run(~r/^([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}$/, query) do
      [_, metric, labels_str] ->
        {metric, parse_labels(labels_str)}

      _ ->
        case Regex.run(~r/^\{([^}]*)\}$/, query) do
          [_, labels_str] ->
            {nil, parse_labels(labels_str)}

          _ ->
            # Plain metric name, no labels
            case Regex.run(~r/^([a-zA-Z_:][a-zA-Z0-9_:]*)$/, query) do
              [_, metric] -> {metric, %{}}
              _ -> {query, %{}}
            end
        end
    end
  end

  defp parse_labels(""), do: %{}

  defp parse_labels(str) do
    str
    |> String.split(",", trim: true)
    |> Enum.map(fn pair ->
      pair = String.trim(pair)

      cond do
        # Regex match: label=~"pattern" or label=~'pattern'
        match = Regex.run(~r/^([a-zA-Z_][a-zA-Z0-9_]*)\s*=~\s*["']([^"']*)["']$/, pair) ->
          [_, k, v] = match
          {k, {:regex, v}}

        # Negative regex: label!~"pattern" or label!~'pattern'
        match = Regex.run(~r/^([a-zA-Z_][a-zA-Z0-9_]*)\s*!~\s*["']([^"']*)["']$/, pair) ->
          [_, k, v] = match
          {k, {:not_regex, v}}

        # Not-equal: label!="value" or label!='value'
        match = Regex.run(~r/^([a-zA-Z_][a-zA-Z0-9_]*)\s*!=\s*["']([^"']*)["']$/, pair) ->
          [_, k, v] = match
          {k, {:not_equal, v}}

        # Exact match: label="value" or label='value'
        match = Regex.run(~r/^([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*["']([^"']*)["']$/, pair) ->
          [_, k, v] = match
          {k, v}

        true ->
          nil
      end
    end)
    |> Enum.reject(&is_nil/1)
    |> Map.new()
  end

  defp duration_to_seconds(n, "s"), do: n
  defp duration_to_seconds(n, "m"), do: n * 60
  defp duration_to_seconds(n, "h"), do: n * 3600
  defp duration_to_seconds(n, "d"), do: n * 86400

  # Map PromQL functions to TimelessMetrics aggregates
  defp map_inner_function(nil), do: :avg
  defp map_inner_function(:avg_over_time), do: :avg
  defp map_inner_function(:min_over_time), do: :min
  defp map_inner_function(:max_over_time), do: :max
  defp map_inner_function(:sum_over_time), do: :sum
  defp map_inner_function(:count_over_time), do: :count
  defp map_inner_function(:rate), do: :rate
  defp map_inner_function(:irate), do: :rate
end
