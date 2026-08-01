# VictoriaMetrics differential harness — the referee for PromQL parity.
#
# Starts a THROWAWAY VictoriaMetrics container (podman run --rm on a local
# port; never touches quadlet-managed services), seeds it and an in-process
# timeless store with identical data, runs the query corpus against both,
# and diffs the results: timestamps must match exactly, values within
# relative tolerance.
#
# Usage:
#     mix run --no-start scripts/vm_diff.exs
#     TIMELESS_VM_DIFF_ENGINE=libsql mix run --no-start scripts/vm_diff.exs
#
# Exit code 0 = full parity on the corpus; 1 = diffs found (printed).

for app <- [:telemetry, :exqlite, :ezstd, :ex_alp, :ex_openzl, :rustler, :rocket, :inets] do
  Application.ensure_all_started(app)
end

defmodule VMDiff do
  @vm_image "docker.io/victoriametrics/victoria-metrics:latest"
  @vm_port 18_928
  @tl_port 18_929
  @container "timeless_vm_diff"
  @tolerance 1.0e-6

  def run do
    now = System.os_time(:second)
    # Aligned, historical enough to dodge VM's search.latencyOffset (30s)
    base = div(now, 60) * 60 - 3600
    q_start = base
    q_end = base + 2400
    step = 60

    start_vm!()
    {:ok, timeless_sup, data_dir} = start_timeless()

    try do
      await_vm!()
      seed = seed_lines(base)
      import_both!(seed)

      corpus = corpus(base)
      results = Enum.map(corpus, &compare(&1, q_start, q_end, step))

      IO.puts("\n--- instant queries (/api/v1/query) ---")
      instant_results = Enum.map(instant_corpus(base), &compare_instant(&1, q_end))
      results = results ++ instant_results

      IO.puts("\n--- metadata endpoints ---")

      meta_paths = [
        "/api/v1/labels",
        "/api/v1/labels?match%5B%5D=g_ramp",
        "/api/v1/labels?match%5B%5D=" <> URI.encode_www_form(~s|host_info{host="a"}|),
        "/api/v1/label/host/values",
        "/api/v1/label/host/values?match%5B%5D=g_sparse",
        "/api/v1/label/le/values?match%5B%5D=lat_bucket",
        "/api/v1/label/__name__/values?match%5B%5D=" <> URI.encode_www_form(~s|{host="a"}|),
        "/api/v1/series?match%5B%5D=g_const&match%5B%5D=g_sparse"
      ]

      meta_results = Enum.map(meta_paths, &compare_meta/1)
      results = results ++ meta_results

      diffs = Enum.reject(results, fn {_q, verdict} -> verdict == :ok end)

      IO.puts("\n=== VM differential summary ===")

      IO.puts(
        "#{length(results) - length(diffs)}/#{length(results)} queries match VictoriaMetrics"
      )

      Enum.each(diffs, fn {q, verdict} ->
        IO.puts("\nDIFF  #{q}")
        print_verdict(verdict)
      end)

      if diffs == [], do: :ok, else: :diffs
    after
      Supervisor.stop(timeless_sup)
      File.rm_rf!(data_dir)
      System.cmd("podman", ["stop", "-t", "2", @container], stderr_to_stdout: true)
    end
  end

  # --- environment ---

  defp start_vm! do
    # --rm: container self-deletes on stop. Never touches other containers.
    {out, code} =
      System.cmd(
        "podman",
        [
          "run",
          "--rm",
          "-d",
          "--name",
          @container,
          "-p",
          "127.0.0.1:#{@vm_port}:8428",
          @vm_image,
          "-search.latencyOffset=1s",
          "-retentionPeriod=1y"
        ],
        stderr_to_stdout: true
      )

    if code != 0, do: raise("failed to start VictoriaMetrics container: #{out}")
    IO.puts("started throwaway VM container on :#{@vm_port}")
  end

  defp start_timeless do
    data_dir = "/tmp/timeless_vm_diff_#{System.os_time(:millisecond)}"
    engine = timeless_engine()
    IO.puts("starting TimelessMetrics engine=#{inspect(engine)} on :#{@tl_port}")

    case Supervisor.start_link(
           [
             {TimelessMetrics,
              name: :vm_diff,
              data_dir: data_dir,
              engine: engine,
              self_monitor: false,
              scraping: false},
             {TimelessMetrics.HTTP, store: :vm_diff, port: @tl_port}
           ],
           strategy: :one_for_one
         ) do
      {:ok, supervisor} -> {:ok, supervisor, data_dir}
      {:error, _} = error -> error
    end
  end

  defp timeless_engine do
    case System.get_env("TIMELESS_VM_DIFF_ENGINE", "rust") do
      "rust" -> :rust
      "libsql" -> :libsql
      other -> raise "TIMELESS_VM_DIFF_ENGINE must be rust or libsql, got: #{inspect(other)}"
    end
  end

  defp await_vm!(tries \\ 60) do
    case http_get("http://127.0.0.1:#{@vm_port}/health") do
      {:ok, _} ->
        :ok

      _ when tries > 0 ->
        Process.sleep(500)
        await_vm!(tries - 1)

      _ ->
        raise "VictoriaMetrics container did not become healthy"
    end
  end

  # --- seeding ---

  # Prometheus exposition lines with explicit ms timestamps. Shapes chosen to
  # exercise semantics: ramping gauge, counter with a reset, sparse series,
  # constant, and a second metric for vector-vector joins.
  defp seed_lines(base) do
    lines =
      for host <- ["a", "b"], i <- 0..239 do
        ts_ms = (base + i * 15) * 1000
        ramp = 10.0 + i * 1.5 + if(host == "a", do: 0.0, else: 100.0)

        counter =
          if i < 120, do: i * 150.0, else: (i - 120) * 150.0

        [
          ~s|g_ramp{host="#{host}"} #{ramp} #{ts_ms}|,
          ~s|c_reqs{host="#{host}"} #{counter} #{ts_ms}|,
          ~s|g_const{host="#{host}"} 42 #{ts_ms}|
        ]
      end

    sparse =
      for i <- 0..29 do
        ts_ms = (base + i * 120) * 1000
        ~s|g_sparse{host="a"} #{5.0 + i} #{ts_ms}|
      end

    # classic histogram: latencies drifting up over time
    hist =
      for host <- ["a", "b"], i <- 0..239 do
        ts_ms = (base + i * 15) * 1000
        # per-step observations: mostly fast, tail grows with i
        c01 = i * 5
        c05 = i * 8 + div(i * i, 500)
        c1 = i * 9 + div(i * i, 300)
        cinf = i * 10 + div(i * i, 200)

        [
          ~s|lat_bucket{host="#{host}",le="0.1"} #{c01} #{ts_ms}|,
          ~s|lat_bucket{host="#{host}",le="0.5"} #{c05} #{ts_ms}|,
          ~s|lat_bucket{host="#{host}",le="1"} #{c1} #{ts_ms}|,
          ~s|lat_bucket{host="#{host}",le="+Inf"} #{cinf} #{ts_ms}|,
          ~s|lat_count{host="#{host}"} #{cinf} #{ts_ms}|
        ]
      end

    # info-style metric (one per host, extra label) and a per-host+core
    # metric (many series per host) to exercise group_left
    extra =
      for host <- ["a", "b"], i <- 0..239 do
        ts_ms = (base + i * 15) * 1000

        [
          ~s|host_info{host="#{host}",os="linux-#{host}"} 1 #{ts_ms}|,
          ~s|core_load{host="#{host}",core="0"} #{1.0 + i * 0.01} #{ts_ms}|,
          ~s|core_load{host="#{host}",core="1"} #{2.0 + i * 0.01} #{ts_ms}|
        ]
      end

    Enum.join(List.flatten(lines) ++ sparse ++ List.flatten(extra) ++ List.flatten(hist), "\n")
  end

  defp import_both!(body) do
    {:ok, _} = http_post("http://127.0.0.1:#{@vm_port}/api/v1/import/prometheus", body)
    {:ok, _} = http_post("http://127.0.0.1:#{@tl_port}/api/v1/import/prometheus", body)

    http_get("http://127.0.0.1:#{@vm_port}/internal/force_flush")
    TimelessMetrics.flush(:vm_diff)
    Process.sleep(500)
  end

  # --- corpus ---

  defp corpus(base) do
    _ = base

    [
      "g_ramp",
      ~s|g_ramp{host="a"}|,
      ~s|g_ramp{host!="a"}|,
      ~s|g_ramp{host=~"a\|b"}|,
      ~s|g_ramp{host!~"b"}|,
      "g_sparse",
      "g_const",
      "rate(c_reqs[1m])",
      "rate(c_reqs[5m])",
      "increase(c_reqs[5m])",
      "irate(c_reqs[1m])",
      "avg_over_time(g_ramp[1m])",
      "avg_over_time(g_ramp[5m])",
      "min_over_time(g_ramp[5m])",
      "max_over_time(g_ramp[5m])",
      "sum_over_time(g_ramp[1m])",
      "count_over_time(g_ramp[5m])",
      "last_over_time(g_ramp[5m])",
      "sum(g_ramp)",
      "avg(g_ramp)",
      "min(g_ramp)",
      "max(g_ramp)",
      "count(g_ramp)",
      "sum by (host) (g_ramp)",
      "avg by (host) (g_ramp)",
      "sum without (host) (g_ramp)",
      "quantile(0.5, g_ramp)",
      "topk(1, g_ramp)",
      "stddev(g_ramp)",
      "g_ramp / 10",
      "g_ramp * 2 + 1",
      "100 - g_ramp",
      "g_ramp % 7",
      "g_ramp ^ 2",
      "g_ramp / g_const",
      "g_ramp > 100",
      "g_ramp > bool 100",
      "g_ramp == 42",
      "g_ramp and g_const",
      "g_ramp unless g_const",
      "g_ramp offset 5m",
      "rate(c_reqs[5m] offset 5m)",
      "abs(g_ramp - 100)",
      "ceil(g_ramp / 10)",
      "floor(g_ramp / 10)",
      "round(g_ramp)",
      "sqrt(g_ramp)",
      "clamp_max(g_ramp, 50)",
      "clamp_min(g_ramp, 50)",
      "clamp(g_ramp, 20, 80)",
      "(avg(g_ramp)) / 10",
      "sum by (host) (rate(c_reqs[5m]))",
      # name-policy probes: which functions keep __name__ in VM?
      "abs(g_ramp)",
      "ceil(g_ramp)",
      "floor(g_ramp)",
      "exp(g_ramp / 100)",
      "ln(g_ramp)",
      "log2(g_ramp)",
      "log10(g_ramp)",
      "round(g_ramp, 10)",
      # additional coverage
      "bottomk(1, g_ramp)",
      "stdvar(g_ramp)",
      "group(g_ramp)",
      "-g_ramp",
      "g_ramp >= 100",
      "g_ramp != 42",
      ~s|g_ramp{host="a"} or g_ramp{host="b"}|,
      # Phase 2A: gauge rollups + over_time stats
      "delta(g_ramp[5m])",
      "idelta(g_ramp[1m])",
      "deriv(g_ramp[5m])",
      "predict_linear(g_ramp[5m], 600)",
      "changes(g_ramp[5m])",
      "resets(c_reqs[30m])",
      "present_over_time(g_sparse[5m])",
      "quantile_over_time(0.9, g_ramp[5m])",
      "stddev_over_time(g_ramp[5m])",
      "stdvar_over_time(g_ramp[5m])",
      # Phase 2A: math batch
      "sgn(g_ramp - 100)",
      "sin(g_ramp)",
      "cos(g_ramp)",
      "tan(g_ramp)",
      "atan(g_ramp)",
      "asin(g_ramp / 1000)",
      "acos(g_ramp / 1000)",
      "sinh(g_ramp / 100)",
      "cosh(g_ramp / 100)",
      "tanh(g_ramp / 100)",
      "asinh(g_ramp)",
      "acosh(g_ramp)",
      "atanh(g_ramp / 1000)",
      "deg(g_ramp)",
      "rad(g_ramp)",
      "g_ramp + 0 * pi()",
      # Phase 2B: label manipulation + count_values
      ~s|label_replace(g_ramp, "h2", "x-$1", "host", "(.*)")|,
      ~s|label_replace(g_ramp, "host", "", "nope", "missing.*")|,
      ~s|label_join(g_ramp, "hj", "-", "host", "host")|,
      ~s|count_values("v", g_const)|,
      ~s|count_values("v", floor(g_ramp / 100))|,
      # Phase 2C: vector matching
      "g_ramp / on(host) g_const",
      "g_ramp / ignoring(host) g_const",
      "g_ramp * on(host) group_left g_const",
      "core_load / on(host) group_left g_const",
      ~s|core_load * on(host) group_left(os) host_info|,
      "g_const * on(host) group_right core_load",
      "g_ramp > on(host) g_const",
      "g_ramp and on(host) g_const",
      "g_ramp or on(host) g_const",
      "g_ramp unless on(host) g_sparse",
      "sum by (host) (core_load) / on(host) g_const",
      # Phase 2D: histogram_quantile
      "histogram_quantile(0.9, lat_bucket)",
      "histogram_quantile(0.5, lat_bucket)",
      "histogram_quantile(0.99, rate(lat_bucket[5m]))",
      "histogram_quantile(0.5, sum by (le) (rate(lat_bucket[5m])))",
      "histogram_quantile(1.5, lat_bucket)",
      "histogram_quantile(-1, lat_bucket)",
      # Phase 2E/F
      "sort(g_ramp)",
      "sort_desc(g_ramp)",
      "absent(g_ramp)",
      ~s|absent(no_such_metric{host="a"})|,
      "absent_over_time(no_such_metric[5m])",
      "absent_over_time(g_sparse[5m])",
      "time()",
      "time() - g_ramp",
      "timestamp(g_ramp)",
      "scalar(sum(g_ramp))",
      "g_ramp / scalar(g_const)",
      "vector(1)",
      "vector(scalar(avg(g_const)))",
      "minute()",
      "hour()",
      "day_of_week()",
      "day_of_month()",
      "day_of_year()",
      "days_in_month()",
      "month()",
      "year()",
      "hour(g_ramp * 0 + 1700000000)",
      # Phase 3: MetricsQL tier
      "(g_ramp > 200) default 0",
      "(g_ramp > 200) default g_const",
      "g_ramp if g_const",
      "g_ramp ifnot g_sparse",
      "rate(c_reqs[5m]) keep_metric_names",
      ~s|alias(g_ramp, "renamed")|,
      ~s|label_set(g_ramp, "dc", "east")|,
      ~s|label_del(g_ramp, "host")|,
      "union(g_ramp, g_const)",
      "default_rollup(g_ramp)",
      "range_avg(g_ramp)",
      "range_max(g_ramp)",
      "running_max(g_ramp)",
      "running_avg(g_ramp)",
      # Phase 3: subqueries, @, step durations, window-less rollups
      "max_over_time(rate(c_reqs[1m])[10m:1m])",
      "avg_over_time((g_ramp)[10m:2m])",
      "avg_over_time(g_ramp[3i])",
      "rate(c_reqs)",
      "g_ramp @ #{base + 600}",
      "avg(g_ramp @ end())",
      "g_ramp offset 5m @ #{base + 600}"
    ]
  end

  # --- instant queries ---

  defp instant_corpus(base) do
    _ = base

    [
      "g_ramp",
      ~s|g_ramp{host="a"}|,
      "g_sparse",
      "rate(c_reqs[5m])",
      "sum(g_ramp)",
      "avg by (host) (g_ramp)",
      "g_ramp / 10",
      "g_ramp > 100",
      "histogram_quantile(0.9, lat_bucket)",
      "topk(1, g_ramp)",
      "time()",
      "vector(1)",
      "absent(no_such_metric)",
      "g_ramp offset 5m",
      "last_over_time(g_ramp[5m])",
      "no_such_metric"
    ]
  end

  defp compare_instant(query, eval_time) do
    vm = query_instant("http://127.0.0.1:#{@vm_port}", query, eval_time)
    tl = query_instant("http://127.0.0.1:#{@tl_port}", query, eval_time)

    verdict =
      case {vm, tl} do
        {{:ok, vm_body}, {:ok, tl_body}} ->
          diff_instant_bodies(vm_body, tl_body)

        {{:error, {vm_code, _}}, {:error, {tl_code, _}}}
        when vm_code in 400..499 and tl_code in 400..499 ->
          :ok

        {vm_err, tl_err} ->
          {:transport, vm_err, tl_err}
      end

    tag = if verdict == :ok, do: "ok  ", else: "DIFF"
    IO.puts("#{tag}  [instant] #{query}")
    {"[instant] " <> query, verdict}
  end

  defp query_instant(base_url, query, time) do
    http_get("#{base_url}/api/v1/query?query=#{URI.encode_www_form(query)}&time=#{time}")
  end

  defp diff_instant_bodies(vm_body, tl_body) do
    vm_json = :json.decode(vm_body)
    tl_json = :json.decode(tl_body)

    cond do
      vm_json["status"] != tl_json["status"] ->
        {:status_mismatch, vm_json["status"], tl_json["status"]}

      vm_json["status"] == "error" ->
        :ok

      vm_json["data"]["resultType"] != tl_json["data"]["resultType"] ->
        {:result_type_mismatch, vm_json["data"]["resultType"], tl_json["data"]["resultType"]}

      true ->
        vm_map =
          Map.new(vm_json["data"]["result"], fn s ->
            {canon_labels(s["metric"]), s["value"]}
          end)

        tl_map =
          Map.new(tl_json["data"]["result"], fn s ->
            {canon_labels(s["metric"]), s["value"]}
          end)

        missing = Map.keys(vm_map) -- Map.keys(tl_map)
        extra = Map.keys(tl_map) -- Map.keys(vm_map)

        value_diffs =
          for {labels, [vm_ts, vm_v]} <- vm_map,
              [tl_ts, tl_v] = Map.get(tl_map, labels),
              tl_ts != nil,
              vm_ts != tl_ts or not same_value?(parse_val(vm_v), parse_val(tl_v)) do
            {labels, {vm_ts, vm_v}, {tl_ts, tl_v}}
          end

        if missing == [] and extra == [] and value_diffs == [] do
          :ok
        else
          {:mismatch, missing: missing, extra: extra, point_diffs: value_diffs}
        end
    end
  end

  # --- metadata endpoints (labels / label values / series) ---

  defp compare_meta(path) do
    vm = http_get("http://127.0.0.1:#{@vm_port}#{path}")
    tl = http_get("http://127.0.0.1:#{@tl_port}#{path}")

    verdict =
      case {vm, tl} do
        {{:ok, vm_body}, {:ok, tl_body}} ->
          vm_data = :json.decode(vm_body)["data"] |> canon_meta()
          tl_data = :json.decode(tl_body)["data"] |> canon_meta()
          if vm_data == tl_data, do: :ok, else: {:meta_mismatch, vm_data, tl_data}

        {vm_err, tl_err} ->
          {:transport, vm_err, tl_err}
      end

    tag = if verdict == :ok, do: "ok  ", else: "DIFF"
    IO.puts("#{tag}  [meta] #{path}")
    {"[meta] " <> path, verdict}
  end

  defp canon_meta(data) when is_list(data) do
    data
    |> Enum.map(fn
      m when is_map(m) ->
        m |> Enum.sort() |> Enum.map(fn {k, v} -> "#{k}=#{v}" end) |> Enum.join(",")

      other ->
        other
    end)
    |> Enum.sort()
  end

  defp canon_meta(other), do: other

  # --- comparison ---

  defp compare(query, q_start, q_end, step) do
    vm = query_range("http://127.0.0.1:#{@vm_port}", query, q_start, q_end, step)
    tl = query_range("http://127.0.0.1:#{@tl_port}", query, q_start, q_end, step)

    verdict =
      case {vm, tl} do
        {{:ok, vm_body}, {:ok, tl_body}} ->
          diff_bodies(vm_body, tl_body)

        # both reject the query (e.g. many-to-many matching) — that's parity
        {{:error, {vm_code, _}}, {:error, {tl_code, _}}}
        when vm_code in 400..499 and tl_code in 400..499 ->
          :ok

        {vm_err, tl_err} ->
          {:transport, vm_err, tl_err}
      end

    tag = if verdict == :ok, do: "ok  ", else: "DIFF"
    IO.puts("#{tag}  #{query}")
    {query, verdict}
  end

  defp query_range(base_url, query, q_start, q_end, step) do
    url =
      "#{base_url}/api/v1/query_range?query=#{URI.encode_www_form(query)}&start=#{q_start}&end=#{q_end}&step=#{step}"

    http_get(url)
  end

  defp diff_bodies(vm_body, tl_body) do
    vm_json = :json.decode(vm_body)
    tl_json = :json.decode(tl_body)

    cond do
      vm_json["status"] != tl_json["status"] ->
        {:status_mismatch, vm_json["status"], tl_json["status"]}

      vm_json["status"] == "error" ->
        :ok

      true ->
        diff_result(vm_json["data"]["result"], tl_json["data"]["result"])
    end
  end

  defp diff_result(vm_series, tl_series) do
    vm_map = Map.new(vm_series, fn s -> {canon_labels(s["metric"]), values_map(s["values"])} end)
    tl_map = Map.new(tl_series, fn s -> {canon_labels(s["metric"]), values_map(s["values"])} end)

    missing = Map.keys(vm_map) -- Map.keys(tl_map)
    extra = Map.keys(tl_map) -- Map.keys(vm_map)

    point_diffs =
      for {labels, vm_vals} <- vm_map,
          tl_vals = Map.get(tl_map, labels),
          tl_vals != nil,
          diff = diff_values(vm_vals, tl_vals),
          diff != [] do
        {labels, diff}
      end

    if missing == [] and extra == [] and point_diffs == [] do
      :ok
    else
      {:mismatch, missing: missing, extra: extra, point_diffs: point_diffs}
    end
  end

  defp canon_labels(m),
    do: m |> Enum.sort() |> Enum.map(fn {k, v} -> "#{k}=#{v}" end) |> Enum.join(",")

  defp values_map(values), do: Map.new(values, fn [ts, v] -> {ts, parse_val(v)} end)

  defp parse_val("+Inf"), do: :inf
  defp parse_val("-Inf"), do: :neg_inf
  defp parse_val("NaN"), do: :nan

  defp parse_val(s) do
    {f, _} = Float.parse(s)
    f
  end

  defp diff_values(vm_vals, tl_vals) do
    ts_all = Map.keys(vm_vals) |> MapSet.new() |> MapSet.union(MapSet.new(Map.keys(tl_vals)))

    ts_all
    |> Enum.sort()
    |> Enum.flat_map(fn ts ->
      vm_v = Map.get(vm_vals, ts, :absent)
      tl_v = Map.get(tl_vals, ts, :absent)

      if same_value?(vm_v, tl_v), do: [], else: [{ts, vm_v, tl_v}]
    end)
  end

  defp same_value?(v, v), do: true

  defp same_value?(a, b) when is_number(a) and is_number(b) do
    abs(a - b) <= @tolerance * max(abs(a), max(abs(b), 1.0))
  end

  defp same_value?(_, _), do: false

  defp print_verdict({:mismatch, missing: m, extra: e, point_diffs: pd}) do
    if m != [], do: IO.puts("  series only in VM:       #{inspect(m)}")
    if e != [], do: IO.puts("  series only in timeless: #{inspect(e)}")

    Enum.each(pd, fn {labels, diffs} ->
      IO.puts("  {#{labels}}: #{length(diffs)} differing points, first:")

      diffs
      |> Enum.take(3)
      |> Enum.each(fn {ts, vm_v, tl_v} ->
        IO.puts("    ts=#{ts}  vm=#{inspect(vm_v)}  timeless=#{inspect(tl_v)}")
      end)
    end)
  end

  defp print_verdict(other), do: IO.puts("  #{inspect(other)}")

  # --- tiny http client ---

  defp http_get(url) do
    case :httpc.request(:get, {String.to_charlist(url), []}, [{:timeout, 15_000}], []) do
      {:ok, {{_, code, _}, _, body}} when code in 200..299 -> {:ok, List.to_string(body)}
      {:ok, {{_, code, _}, _, body}} -> {:error, {code, List.to_string(body)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp http_post(url, body) do
    case :httpc.request(
           :post,
           {String.to_charlist(url), [], ~c"text/plain", body},
           [{:timeout, 30_000}],
           []
         ) do
      {:ok, {{_, code, _}, _, resp}} when code in 200..299 -> {:ok, List.to_string(resp)}
      {:ok, {{_, 204, _}, _, _}} -> {:ok, ""}
      {:ok, {{_, code, _}, _, resp}} -> {:error, {code, List.to_string(resp)}}
      {:error, reason} -> {:error, reason}
    end
  end
end

case VMDiff.run() do
  :ok -> IO.puts("\nfull parity ✓")
  :diffs -> System.halt(1)
end
