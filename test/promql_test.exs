defmodule TimelessMetrics.PromQLTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.PromQL

  @store :promql_eval_test
  @data_dir "/tmp/timeless_promql_test_#{System.os_time(:millisecond)}"
  @base_ts 1_700_000_000

  # Convenience: unwrap the selector map from a parse result
  defp sel!(query) do
    {:ok, {:selector, sel}} = PromQL.parse(query)
    sel
  end

  defp host_value("a"), do: 10.0
  defp host_value("b"), do: 20.0
  defp host_value("c"), do: 30.0

  defp run(query) do
    {:ok, ast} = PromQL.parse(query)
    {:ok, response} = PromQL.execute(ast, @store, @base_ts, @base_ts + 100, 100)
    response["data"]["result"]
  end

  defp values(result), do: Enum.map(result["values"], fn [_ts, v] -> String.to_float(v) end)

  describe "parse/1 - simple selectors" do
    test "plain metric name" do
      sel = sel!("cpu_usage_user")
      assert sel.metric == "cpu_usage_user"
      assert sel.labels == %{}
      assert sel.pattern == nil
    end

    test "metric with exact label" do
      sel = sel!(~s|cpu_usage_user{hostname="host_0"}|)
      assert sel.metric == "cpu_usage_user"
      assert sel.labels == %{"hostname" => "host_0"}
    end

    test "metric with multiple exact labels" do
      sel = sel!(~s|cpu_usage_user{hostname="host_0",region="us-east"}|)
      assert sel.metric == "cpu_usage_user"
      assert sel.labels == %{"hostname" => "host_0", "region" => "us-east"}
    end

    test "metric with regex label" do
      sel = sel!(~s|cpu_usage_user{hostname=~"host_0\|host_1"}|)
      assert sel.labels == %{"hostname" => {:regex, "host_0|host_1"}}
    end

    test "mixed exact and regex labels" do
      sel = sel!(~s|cpu_usage_user{hostname=~"host_0\|host_1",region="us-east"}|)

      assert sel.labels == %{
               "hostname" => {:regex, "host_0|host_1"},
               "region" => "us-east"
             }
    end

    test "negative matchers" do
      sel = sel!(~s|cpu{host!="a",env!~"prod.*"}|)
      assert sel.labels == %{"host" => {:not_equal, "a"}, "env" => {:not_regex, "prod.*"}}
    end

    test "label-only selector matches all metrics" do
      sel = sel!(~s|{host="a"}|)
      assert sel.metric == nil
      assert sel.pattern == ".+"
      assert sel.labels == %{"host" => "a"}
    end
  end

  describe "parse/1 - range vector functions" do
    test "max_over_time with 1h range" do
      {:ok, ast} = PromQL.parse("max_over_time(cpu_usage_user{hostname=\"host_0\"}[1h])")
      assert {:call, :max_over_time, [{:range, {:selector, sel}, 3600}]} = ast
      assert sel.metric == "cpu_usage_user"
      assert sel.labels == %{"hostname" => "host_0"}
    end

    test "avg_over_time with 5m range" do
      {:ok, ast} = PromQL.parse("avg_over_time(cpu_usage_user{hostname=\"host_0\"}[5m])")
      assert {:call, :avg_over_time, [{:range, _, 300}]} = ast
    end

    test "rate function" do
      {:ok, ast} = PromQL.parse("rate(cpu_usage_user{hostname=\"host_0\"}[1m])")
      assert {:call, :rate, [{:range, _, 60}]} = ast
    end

    test "last_over_time (bug report regression)" do
      {:ok, ast} = PromQL.parse("last_over_time(cpu_usage[5m])")
      assert {:call, :last_over_time, [{:range, {:selector, %{metric: "cpu_usage"}}, 300}]} = ast
    end

    test "compound durations" do
      {:ok, ast} = PromQL.parse("rate(m[1h30m])")
      assert {:call, :rate, [{:range, _, 5400}]} = ast
    end
  end

  describe "parse/1 - aggregation grouping" do
    test "suffix by clause (TSBS shape)" do
      {:ok, ast} =
        PromQL.parse(
          "max(max_over_time(cpu_usage_user{hostname=~\"host_0|host_1\"}[1h])) by (hostname)"
        )

      assert {:agg, :max, {:by, ["hostname"]}, nil, {:call, :max_over_time, _}} = ast
    end

    test "prefix by clause (Grafana shape, bug report regression)" do
      {:ok, ast} = PromQL.parse("avg by (host) (cpu_usage)")
      assert {:agg, :avg, {:by, ["host"]}, nil, {:selector, %{metric: "cpu_usage"}}} = ast
    end

    test "bare aggregation with no grouping" do
      {:ok, ast} = PromQL.parse("avg(cpu_usage)")
      assert {:agg, :avg, nil, nil, {:selector, %{metric: "cpu_usage"}}} = ast
    end

    test "without clause" do
      {:ok, ast} = PromQL.parse("sum without (cpu) (rate(cpu_usage[1m]))")
      assert {:agg, :sum, {:without, ["cpu"]}, nil, {:call, :rate, _}} = ast
    end

    test "multiple group-by keys" do
      {:ok, ast} =
        PromQL.parse(
          "avg(avg_over_time(cpu_usage_user{hostname=~\"host_0\"}[1h])) by (hostname, region)"
        )

      assert {:agg, :avg, {:by, ["hostname", "region"]}, nil, _} = ast
    end

    test "parameterized aggregations" do
      assert {:ok, {:agg, :topk, nil, {:number, 3.0}, _}} = PromQL.parse("topk(3, cpu_usage)")

      assert {:ok, {:agg, :quantile, nil, {:number, 0.9}, _}} =
               PromQL.parse("quantile(0.9, cpu_usage)")
    end
  end

  describe "parse/1 - binary operators" do
    test "scalar division (bug report regression)" do
      {:ok, ast} = PromQL.parse("cpu_usage / 10")
      assert {:binop, :div, false, {:selector, %{metric: "cpu_usage"}}, {:number, 10.0}} = ast
    end

    test "scalar multiplication" do
      {:ok, ast} = PromQL.parse("ifHCInOctets * 8")
      assert {:binop, :mul, false, _, {:number, 8.0}} = ast
    end

    test "parenthesized aggregation with arithmetic (bug report regression)" do
      {:ok, ast} = PromQL.parse("(avg(cpu_usage)) / 10")
      assert {:binop, :div, false, {:agg, :avg, nil, nil, _}, {:number, 10.0}} = ast
    end

    test "threshold comparison" do
      {:ok, ast} =
        PromQL.parse(
          "max(max_over_time(cpu_usage_user{hostname=~\"host_0\"}[1h])) by (hostname) > 90"
        )

      assert {:binop, :gt, false, {:agg, :max, {:by, ["hostname"]}, nil, _}, {:number, 90.0}} =
               ast
    end

    test "less-than threshold" do
      {:ok, ast} =
        PromQL.parse(
          "max(max_over_time(cpu_usage_user{hostname=\"host_0\"}[1h])) by (hostname) < 10.5"
        )

      assert {:binop, :lt, false, _, {:number, 10.5}} = ast
    end

    test "bool modifier" do
      {:ok, ast} = PromQL.parse("cpu_usage > bool 90")
      assert {:binop, :gt, true, _, {:number, 90.0}} = ast
    end

    test "operator precedence: mul binds tighter than add" do
      {:ok, ast} = PromQL.parse("a + b * 2")
      assert {:binop, :add, false, _, {:binop, :mul, false, _, {:number, 2.0}}} = ast
    end

    test "vector-vector arithmetic parses" do
      {:ok, ast} = PromQL.parse("errors_total / requests_total")
      assert {:binop, :div, false, {:selector, _}, {:selector, _}} = ast
    end
  end

  describe "parse/1 - multi-metric via __name__" do
    test "__name__ regex pattern" do
      {:ok, ast} =
        PromQL.parse(
          "max(max_over_time({__name__=~\"cpu_.*\",hostname=~\"host_0\"}[1h])) by (hostname)"
        )

      assert {:agg, :max, {:by, ["hostname"]}, nil,
              {:call, :max_over_time, [{:range, {:selector, sel}, 3600}]}} = ast

      assert sel.metric == nil
      assert sel.pattern == "cpu_.*"
      assert sel.labels == %{"hostname" => {:regex, "host_0"}}
    end
  end

  describe "parse/1 - quoting and whitespace" do
    test "exact match with single quotes" do
      assert sel!("cpu_usage_user{hostname='host_0'}").labels == %{"hostname" => "host_0"}
    end

    test "regex match with single quotes" do
      assert sel!("cpu_usage_user{hostname=~'host_0|host_1'}").labels ==
               %{"hostname" => {:regex, "host_0|host_1"}}
    end

    test "mixed single and double quotes" do
      assert sel!("cpu{hostname='host_0',region=\"us-east\"}").labels ==
               %{"hostname" => "host_0", "region" => "us-east"}
    end

    test "empty label selector" do
      sel = sel!("cpu_usage_user{}")
      assert sel.metric == "cpu_usage_user"
      assert sel.labels == %{}
    end

    test "spaces after commas in labels" do
      assert sel!("cpu{hostname='host_0', region='us-east'}").labels ==
               %{"hostname" => "host_0", "region" => "us-east"}
    end

    test "trailing comma in matchers" do
      assert sel!("cpu{hostname='host_0',}").labels == %{"hostname" => "host_0"}
    end
  end

  describe "parse/1 - offset" do
    test "selector offset" do
      assert sel!("cpu_usage offset 5m").offset == 300
    end

    test "range offset" do
      {:ok, ast} = PromQL.parse("rate(cpu_usage[5m] offset 1h)")
      assert {:call, :rate, [{:range, {:selector, %{offset: 3600}}, 300}]} = ast
    end
  end

  describe "parse/1 - TSBS DevOps query patterns" do
    test "SingleGroupby: 1-host-1-hr" do
      {:ok, ast} =
        PromQL.parse("max(max_over_time(cpu_usage_user{hostname=~\"host_0\"}[1h])) by (hostname)")

      assert {:agg, :max, {:by, ["hostname"]}, nil,
              {:call, :max_over_time, [{:range, {:selector, sel}, 3600}]}} = ast

      assert sel.metric == "cpu_usage_user"
      assert sel.labels == %{"hostname" => {:regex, "host_0"}}
    end

    test "SingleGroupby-5: TSBS actual format with single quotes and grouped regex" do
      {:ok, ast} =
        PromQL.parse(
          "max(max_over_time({__name__=~'cpu_(usage_user|usage_system|usage_idle|usage_nice|usage_iowait)', hostname='host_35'}[1m])) by (__name__)"
        )

      assert {:agg, :max, {:by, ["__name__"]}, nil,
              {:call, :max_over_time, [{:range, {:selector, sel}, 60}]}} = ast

      assert sel.pattern == "cpu_(usage_user|usage_system|usage_idle|usage_nice|usage_iowait)"
      assert sel.labels == %{"hostname" => "host_35"}
    end

    test "DoubleGroupby: 5-metrics-1-host-1-hr" do
      {:ok, ast} =
        PromQL.parse(
          "max(max_over_time({__name__=~\"cpu_usage_user|cpu_usage_system|cpu_usage_idle|cpu_usage_nice|cpu_usage_iowait\",hostname=~\"host_0\"}[1h])) by (hostname)"
        )

      assert {:agg, :max, {:by, ["hostname"]}, nil,
              {:call, :max_over_time, [{:range, {:selector, sel}, 3600}]}} = ast

      assert sel.pattern ==
               "cpu_usage_user|cpu_usage_system|cpu_usage_idle|cpu_usage_nice|cpu_usage_iowait"
    end

    test "MaxAllCPU: all-cpu-metrics-8-hosts-1-hr" do
      {:ok, ast} =
        PromQL.parse(
          "max(max_over_time({__name__=~\"cpu_.*\",hostname=~\"host_0|host_1|host_2|host_3|host_4|host_5|host_6|host_7\"}[1h])) by (hostname)"
        )

      assert {:agg, :max, {:by, ["hostname"]}, nil,
              {:call, :max_over_time, [{:range, {:selector, sel}, 3600}]}} = ast

      assert sel.pattern == "cpu_.*"
    end

    test "HighCPU: cpu > threshold" do
      {:ok, ast} =
        PromQL.parse(
          "max(max_over_time(cpu_usage_user{hostname=~\"host_.*\"}[1h])) by (hostname) > 90"
        )

      assert {:binop, :gt, false, {:agg, :max, {:by, ["hostname"]}, nil, _}, {:number, 90.0}} =
               ast
    end
  end

  describe "parse/1 - errors instead of silent empty results" do
    test "empty query" do
      assert {:error, _} = PromQL.parse("")
      assert {:error, _} = PromQL.parse("   ")
    end

    test "unknown function is an error, not a metric lookup" do
      assert {:error, msg} = PromQL.parse("foobar(cpu_usage)")
      assert msg =~ "unknown function"
    end

    test "recognized-but-unsupported functions name themselves" do
      assert {:error, msg} = PromQL.parse("histogram_quantile(0.9, foo_bucket)")
      assert msg =~ "histogram_quantile"
      assert msg =~ "not supported"
    end

    test "dangling operator" do
      assert {:error, _} = PromQL.parse("cpu_usage /")
    end

    test "malformed matcher" do
      assert {:error, _} = PromQL.parse("cpu_usage{host=}")
    end

    test "unterminated string" do
      assert {:error, msg} = PromQL.parse(~s|cpu{host="a}|)
      assert msg =~ "unterminated string"
    end

    test "trailing garbage after expression" do
      assert {:error, _} = PromQL.parse("cpu_usage cpu_usage")
    end

    test "vector matching modifiers are rejected with a clear message" do
      assert {:error, msg} = PromQL.parse("a / on (host) b")
      assert msg =~ "on/ignoring"
    end

    test "subqueries are rejected" do
      assert {:error, msg} = PromQL.parse("(rate(m[5m]))[30m]")
      assert msg =~ "subquer"
    end

    test "@ modifier is rejected" do
      assert {:error, msg} = PromQL.parse("cpu_usage @ 1609746000")
      assert msg =~ "@"
    end

    test "count_values is rejected with a clear message" do
      assert {:error, msg} = PromQL.parse(~s|count_values("version", build_info)|)
      assert msg =~ "count_values"
    end
  end

  describe "selector_info/1" do
    test "plain selector" do
      {:ok, ast} = PromQL.parse(~s|cpu{host="a"}|)
      info = PromQL.selector_info(ast)
      assert info.metric == "cpu"
      assert info.metric_pattern == nil
      assert info.labels == %{"host" => "a"}
    end

    test "nested selector inside aggregation" do
      {:ok, ast} = PromQL.parse("max(max_over_time({__name__=~\"cpu_.*\"}[1h])) by (host)")
      info = PromQL.selector_info(ast)
      assert info.metric == nil
      assert info.metric_pattern == "cpu_.*"
    end
  end

  describe "execute/5 - evaluation semantics" do
    setup do
      start_supervised!({TimelessMetrics, name: @store, data_dir: @data_dir})

      # 3 hosts x 2 metrics; constant value per host, points every 10s
      entries =
        for host <- ["a", "b", "c"],
            i <- 0..9,
            {metric, value} <- [{"cpu_usage", host_value(host)}, {"mem_usage", 50.0}] do
          {metric, %{"host" => host}, value, @base_ts + i * 10}
        end

      TimelessMetrics.write_batch(@store, entries)
      TimelessMetrics.flush(@store)

      on_exit(fn -> File.rm_rf!(@data_dir) end)
      :ok
    end

    test "plain selector keeps __name__ and returns one series per host" do
      result = run("cpu_usage")
      assert length(result) == 3
      assert Enum.all?(result, &(&1["metric"]["__name__"] == "cpu_usage"))
    end

    test "bare avg() collapses to a single series and drops __name__ (bug report item 3)" do
      assert [%{"metric" => metric} = single] = run("avg(cpu_usage)")
      assert metric == %{}
      assert values(single) == [20.0]
    end

    test "avg by (host) returns one series per host (bug report item 2)" do
      result = run("avg by (host) (cpu_usage)")
      assert length(result) == 3
      hosts = result |> Enum.map(& &1["metric"]) |> Enum.sort()
      assert hosts == [%{"host" => "a"}, %{"host" => "b"}, %{"host" => "c"}]
    end

    test "suffix by form gives the same result as prefix" do
      assert run("avg by (host) (cpu_usage)") == run("avg(cpu_usage) by (host)")
    end

    test "sum(...) actually sums across series" do
      assert [single] = run("sum(cpu_usage)")
      assert values(single) == [60.0]
    end

    test "last_over_time returns data and keeps __name__ (bug report item 5)" do
      result = run("last_over_time(cpu_usage[5m])")
      assert length(result) == 3
      assert Enum.all?(result, &(&1["metric"]["__name__"] == "cpu_usage"))
      a = Enum.find(result, &(&1["metric"]["host"] == "a"))
      assert values(a) == [10.0]
    end

    test "scalar division scales values and drops __name__ (bug report item 4)" do
      result = run("cpu_usage / 10")
      assert length(result) == 3
      a = Enum.find(result, &(&1["metric"] == %{"host" => "a"}))
      assert values(a) == [1.0]
    end

    test "(avg(cpu_usage)) / 10" do
      assert [single] = run("(avg(cpu_usage)) / 10")
      assert values(single) == [2.0]
    end

    test "scalar on the left side" do
      result = run("100 - cpu_usage")
      a = Enum.find(result, &(&1["metric"] == %{"host" => "a"}))
      assert values(a) == [90.0]
    end

    test "comparison filters samples" do
      result = run("avg by (host) (cpu_usage) > 15")
      hosts = result |> Enum.map(& &1["metric"]["host"]) |> Enum.sort()
      assert hosts == ["b", "c"]
    end

    test "bool comparison returns 0/1 for every series" do
      result = run("avg by (host) (cpu_usage) > bool 15")
      assert length(result) == 3
      by_host = Map.new(result, &{&1["metric"]["host"], values(&1)})
      assert by_host == %{"a" => [0.0], "b" => [1.0], "c" => [1.0]}
    end

    test "vector-vector arithmetic joins on matching labels" do
      result = run("cpu_usage / mem_usage")
      assert length(result) == 3
      a = Enum.find(result, &(&1["metric"] == %{"host" => "a"}))
      assert values(a) == [0.2]
    end

    test "division by zero yields +Inf, not a crash" do
      assert [single] = run("avg(cpu_usage) / 0")
      assert [[_ts, "+Inf"]] = single["values"]
    end

    test "topk selects the highest series" do
      assert [%{"metric" => %{"host" => "c"}}] = run("topk(1, avg by (host) (cpu_usage))")
    end

    test "quantile aggregation" do
      assert [single] = run("quantile(0.5, avg by (host) (cpu_usage))")
      assert values(single) == [20.0]
    end

    test "range vector without a function is an execution error" do
      {:ok, ast} = PromQL.parse("cpu_usage[5m]")
      assert {:error, msg} = PromQL.execute(ast, @store, @base_ts, @base_ts + 100, 100)
      assert msg =~ "range vector"
    end

    test "clamp_max caps values" do
      result = run("clamp_max(cpu_usage, 12)")
      c = Enum.find(result, &(&1["metric"] == %{"host" => "c"}))
      assert values(c) == [12.0]
    end

    test "and/unless set operators" do
      # cpu_usage and mem_usage share label signatures → intersection keeps all
      assert length(run("cpu_usage and mem_usage")) == 3
      assert run("cpu_usage unless mem_usage") == []
    end
  end
end
