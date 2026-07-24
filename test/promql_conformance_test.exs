defmodule TimelessMetrics.PromQLConformanceTest do
  use ExUnit.Case, async: false

  @moduledoc """
  Pins the PromQL conformance surface documented in
  notes/promql_conformance_audit_2026-07-24.md.

  Every supported construct must parse AND execute; every unsupported
  construct must return a named error — never an empty-success response.
  If you implement a new construct, move it from the rejected list to the
  supported list and update the audit doc.
  """

  alias TimelessMetrics.PromQL

  @store :promql_conformance_test
  @data_dir "/tmp/timeless_promql_conf_#{System.os_time(:millisecond)}"
  @base_ts 1_700_000_000

  setup_all do
    {:ok, sup} =
      Supervisor.start_link(
        [{TimelessMetrics, name: @store, data_dir: @data_dir}],
        strategy: :one_for_one
      )

    entries =
      List.flatten(
        for host <- ["a", "b"], i <- 0..29 do
          [
            {"cpu", %{"host" => host}, 10.0 + i, @base_ts + i * 10},
            {"mem", %{"host" => host}, 50.0, @base_ts + i * 10},
            {"reqs_total", %{"host" => host}, i * 100.0, @base_ts + i * 10}
          ]
        end
      )

    TimelessMetrics.write_batch(@store, entries)
    TimelessMetrics.flush(@store)

    on_exit(fn ->
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end

      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  defp execute(query) do
    case PromQL.parse(query) do
      {:ok, ast} -> PromQL.execute(ast, @store, @base_ts, @base_ts + 300, 60)
      {:error, _} = err -> err
    end
  end

  # ✅ Supported: must parse and execute successfully
  @supported [
    # selectors & matchers
    "cpu",
    ~s|cpu{host="a"}|,
    ~s|cpu{host!="a"}|,
    ~s|cpu{host=~"a\|b"}|,
    ~s|cpu{host!~"a"}|,
    ~s|{__name__=~"cpu\|mem"}|,
    ~s|{host="a"}|,
    # modifiers
    "rate(cpu[90s])",
    "rate(cpu[1h30m])",
    "cpu offset 1m",
    "cpu offset -1m",
    "rate(cpu[1m] offset 1m)",
    # binary operators
    "cpu + 1",
    "cpu - 1",
    "cpu * 2",
    "cpu / 2",
    "cpu % 3",
    "cpu ^ 2",
    "2 ^ cpu",
    "-cpu",
    "cpu / mem",
    "cpu > 15",
    "cpu >= 15",
    "cpu < 15",
    "cpu <= 15",
    "cpu == 15",
    "cpu != 15",
    "cpu > bool 15",
    "1 > bool 2",
    "cpu > mem",
    "cpu and mem",
    "cpu or mem",
    "cpu unless mem",
    # aggregations
    "sum(cpu)",
    "avg(cpu)",
    "min(cpu)",
    "max(cpu)",
    "count(cpu)",
    "group(cpu)",
    "stddev(cpu)",
    "stdvar(cpu)",
    "topk(2, cpu)",
    "bottomk(2, cpu)",
    "quantile(0.9, cpu)",
    "sum by (host) (cpu)",
    "sum(cpu) by (host)",
    "sum without (host) (cpu)",
    "sum by () (cpu)",
    # rollups
    "rate(reqs_total[1m])",
    "irate(reqs_total[1m])",
    "increase(reqs_total[1m])",
    "avg_over_time(cpu[1m])",
    "min_over_time(cpu[1m])",
    "max_over_time(cpu[1m])",
    "sum_over_time(cpu[1m])",
    "count_over_time(cpu[1m])",
    "last_over_time(cpu[1m])",
    "first_over_time(cpu[1m])",
    # transforms
    "abs(cpu)",
    "ceil(cpu)",
    "floor(cpu)",
    "round(cpu)",
    "round(cpu, 5)",
    "sqrt(cpu)",
    "exp(cpu)",
    "ln(cpu)",
    "log2(cpu)",
    "log10(cpu)",
    "clamp(cpu, 0, 20)",
    "clamp_min(cpu, 12)",
    "clamp_max(cpu, 18)",
    # gauge rollups + over_time stats (Phase 2A)
    "delta(cpu[1m])",
    "idelta(cpu[1m])",
    "deriv(cpu[1m])",
    "predict_linear(cpu[1m], 300)",
    "resets(reqs_total[1m])",
    "changes(cpu[1m])",
    "present_over_time(cpu[1m])",
    "quantile_over_time(0.9, cpu[1m])",
    "stddev_over_time(cpu[1m])",
    "stdvar_over_time(cpu[1m])",
    # math batch (Phase 2A)
    "sgn(cpu)",
    "acos(cpu / 100)",
    "asin(cpu / 100)",
    "atan(cpu)",
    "cos(cpu)",
    "sin(cpu)",
    "tan(cpu)",
    "cosh(cpu / 10)",
    "sinh(cpu / 10)",
    "tanh(cpu)",
    "acosh(cpu)",
    "asinh(cpu)",
    "atanh(cpu / 100)",
    "deg(cpu)",
    "rad(cpu)",
    "pi()",
    # label manipulation + count_values (Phase 2B)
    ~s|label_replace(cpu, "h2", "$1", "host", "(.*)")|,
    ~s|label_join(cpu, "hj", "-", "host")|,
    ~s|count_values("v", cpu)|
  ]

  # ❌ Rejected with a message naming the construct (not a generic parse error)
  @rejected_named [
    "cpu @ 1700000000",
    "cpu @ start()",
    "cpu[5m:1m]",
    "(rate(cpu[1m]))[5m:]",
    "cpu + on(host) mem",
    "cpu + ignoring(cpu) mem",
    "cpu / on(host) group_left mem",
    "limitk(2, cpu)",
    "limit_ratio(0.5, cpu)",
    "mad_over_time(cpu[1m])",
    "double_exponential_smoothing(cpu[1m], 0.5, 0.5)",
    "holt_winters(cpu[1m], 0.5, 0.5)",
    "sort(cpu)",
    "sort_desc(cpu)",
    ~s|sort_by_label(cpu, "host")|,
    ~s|sort_by_label_desc(cpu, "host")|,
    "absent(nonexistent)",
    "absent_over_time(nonexistent[5m])",
    "histogram_quantile(0.9, rate(reqs_bucket[5m]))",
    "histogram_fraction(0, 100, reqs_total)",
    "histogram_avg(reqs_total)",
    "histogram_count(reqs_total)",
    "histogram_sum(reqs_total)",
    "histogram_stddev(reqs_total)",
    "scalar(avg(cpu))",
    "vector(1)",
    "time()",
    "timestamp(cpu)",
    "minute()",
    "hour()",
    "day_of_week()",
    "day_of_month()",
    "day_of_year()",
    "days_in_month()",
    "month()",
    "year()",
    "info(cpu)"
  ]

  # ❌ MetricsQL extensions: rejected with a message naming MetricsQL
  @rejected_metricsql [
    "cpu default 0",
    "cpu if mem",
    "default_rollup(cpu)",
    ~s|label_set(cpu, "x", "y")|,
    "range_avg(cpu)",
    ~s|alias(cpu, "c")|,
    "union(cpu, mem)",
    "with (f = cpu) f",
    "rate(cpu[1m]) keep_metric_names"
  ]

  test "all supported constructs parse and execute" do
    failures =
      for query <- @supported,
          result = execute(query),
          not match?({:ok, %{"status" => "success"}}, result) do
        {query, result}
      end

    assert failures == []
  end

  test "supported constructs that select existing data return non-empty results" do
    # A subset where an empty result would indicate a silent regression
    non_empty = [
      "cpu",
      "avg by (host) (cpu)",
      "avg(cpu)",
      "last_over_time(cpu[1m])",
      "cpu / 10",
      "rate(reqs_total[1m])",
      "sum(cpu) by (host)"
    ]

    for query <- non_empty do
      {:ok, resp} = execute(query)
      assert resp["data"]["result"] != [], "#{query} returned empty result"
    end
  end

  test "unsupported constructs return an error naming the construct" do
    failures =
      for query <- @rejected_named do
        case execute(query) do
          {:error, msg} ->
            # The message must name the construct or clearly state unsupported —
            # a bare "unknown function" or generic parse error is a docs gap.
            if msg =~ "not supported" or msg =~ "unknown function" do
              nil
            else
              {query, {:vague_error, msg}}
            end

          {:ok, _} ->
            {query, :unexpectedly_succeeded}
        end
      end
      |> Enum.reject(&is_nil/1)

    assert failures == []
  end

  test "no rejected construct produces a generic 'unknown function' error" do
    # Everything in the rejected list is a real PromQL construct, so it should
    # be recognized ("not supported yet"), not treated as a typo.
    vague =
      for query <- @rejected_named,
          {:error, msg} <- [execute(query)],
          msg =~ "unknown function" do
        {query, msg}
      end

    assert vague == []
  end

  test "MetricsQL extensions are rejected with a MetricsQL-specific message" do
    failures =
      for query <- @rejected_metricsql do
        case execute(query) do
          {:error, msg} ->
            if msg =~ "MetricsQL", do: nil, else: {query, {:wrong_message, msg}}

          {:ok, _} ->
            {query, :unexpectedly_succeeded}
        end
      end
      |> Enum.reject(&is_nil/1)

    assert failures == []
  end

  test "nothing rejected ever returns empty-success" do
    for query <- @rejected_named ++ @rejected_metricsql do
      refute match?({:ok, _}, execute(query)),
             "#{query} returned success — must be an error or a real result"
    end
  end

  # --- Prometheus/VM parity semantics (formerly audit §2 divergences). ---
  # These assert the Phase 1 windowed-evaluator behavior; if one fails, the
  # semantics regressed toward the old bucketed evaluation.

  test "PARITY §2.2: range windows are honored" do
    # cpu ramps 10..39 in 10s steps; a 1m window and a 30m window must
    # produce different averages at the same grid points.
    {:ok, ast1} = PromQL.parse("avg_over_time(cpu[1m])")
    {:ok, ast2} = PromQL.parse("avg_over_time(cpu[30m])")
    {:ok, r1} = PromQL.execute(ast1, @store, @base_ts, @base_ts + 300, 60)
    {:ok, r2} = PromQL.execute(ast2, @store, @base_ts, @base_ts + 300, 60)
    refute r1 == r2

    # Hand-computed: at T = base+300 the (T-60, T] window holds samples
    # i=25..29 (values 35..39) → avg 37.0. avg_over_time keeps __name__
    # (VM name policy).
    a =
      Enum.find(r1["data"]["result"], &(&1["metric"] == %{"__name__" => "cpu", "host" => "a"}))

    [_, last_val] = List.last(a["values"])
    assert String.to_float(last_val) == 37.0
  end

  test "PARITY §2.3: rate is windowed reset-adjusted increase; irate is last-two-samples" do
    # reqs_total grows 100 per 10s (10/s). Data ends at base+290, so the
    # (base+240, base+300] rate window only contains 50s of growth:
    # rate = 500/60 ≈ 8.33/s, while irate (last two samples) stays 10/s.
    {:ok, ast1} = PromQL.parse("rate(reqs_total[1m])")
    {:ok, ast2} = PromQL.parse("irate(reqs_total[1m])")
    {:ok, r1} = PromQL.execute(ast1, @store, @base_ts, @base_ts + 300, 60)
    {:ok, r2} = PromQL.execute(ast2, @store, @base_ts, @base_ts + 300, 60)
    refute r1 == r2

    rate_a = Enum.find(r1["data"]["result"], &(&1["metric"] == %{"host" => "a"}))
    irate_a = Enum.find(r2["data"]["result"], &(&1["metric"] == %{"host" => "a"}))

    [_, rate_last] = List.last(rate_a["values"])
    [_, irate_last] = List.last(irate_a["values"])
    assert_in_delta String.to_float(rate_last), 500.0 / 60, 1.0e-9
    assert_in_delta String.to_float(irate_last), 10.0, 1.0e-9

    # Full-window grid points see the true 10/s rate
    [_, rate_mid] = Enum.at(rate_a["values"], 1)
    assert_in_delta String.to_float(rate_mid), 10.0, 1.0e-9
  end

  test "PARITY §2.6: duplicate matchers are ANDed" do
    {:ok, ast} = PromQL.parse(~s|cpu{host="nope",host="a"}|)
    {:ok, resp} = PromQL.execute(ast, @store, @base_ts, @base_ts + 300, 60)
    assert resp["data"]["result"] == []

    {:ok, ast2} = PromQL.parse(~s|cpu{host=~"a\|b",host!="b"}|)
    {:ok, resp2} = PromQL.execute(ast2, @store, @base_ts, @base_ts + 300, 60)

    assert [%{"host" => "a"}] =
             Enum.map(resp2["data"]["result"], &Map.delete(&1["metric"], "__name__"))
  end

  test "PARITY §2.5: lookback fills steps between sparse samples" do
    # cpu has samples every 10s; a 5s step grid still gets a value at every
    # grid point from the 5m lookback.
    {:ok, ast} = PromQL.parse(~s|cpu{host="a"}|)
    {:ok, resp} = PromQL.execute(ast, @store, @base_ts, @base_ts + 100, 5)
    [%{"values" => values}] = resp["data"]["result"]
    assert length(values) == 21
  end

  test "PARITY §2.7: output timestamps sit exactly on start + n*step" do
    off_grid_start = @base_ts + 7
    {:ok, ast} = PromQL.parse("cpu")
    {:ok, resp} = PromQL.execute(ast, @store, off_grid_start, off_grid_start + 120, 60)

    for %{"values" => values} <- resp["data"]["result"],
        [ts, _v] <- values do
      assert rem(ts - off_grid_start, 60) == 0
    end
  end
end
