defmodule TimelessMetrics.PromQLTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.PromQL

  describe "parse/1 - simple selectors" do
    test "plain metric name" do
      {:ok, plan} = PromQL.parse("cpu_usage_user")
      assert plan.metric == "cpu_usage_user"
      assert plan.labels == %{}
      assert plan.outer_aggregate == nil
      assert plan.inner_function == nil
      assert plan.group_by == nil
      assert plan.threshold == nil
    end

    test "metric with exact label" do
      {:ok, plan} = PromQL.parse(~s|cpu_usage_user{hostname="host_0"}|)
      assert plan.metric == "cpu_usage_user"
      assert plan.labels == %{"hostname" => "host_0"}
    end

    test "metric with multiple exact labels" do
      {:ok, plan} = PromQL.parse(~s|cpu_usage_user{hostname="host_0",region="us-east"}|)
      assert plan.metric == "cpu_usage_user"
      assert plan.labels == %{"hostname" => "host_0", "region" => "us-east"}
    end

    test "metric with regex label" do
      {:ok, plan} = PromQL.parse(~s|cpu_usage_user{hostname=~"host_0\|host_1"}|)
      assert plan.metric == "cpu_usage_user"
      assert plan.labels == %{"hostname" => {:regex, "host_0|host_1"}}
    end

    test "mixed exact and regex labels" do
      {:ok, plan} =
        PromQL.parse(~s|cpu_usage_user{hostname=~"host_0\|host_1",region="us-east"}|)

      assert plan.labels == %{
               "hostname" => {:regex, "host_0|host_1"},
               "region" => "us-east"
             }
    end
  end

  describe "parse/1 - range vector functions" do
    test "max_over_time with 1h range" do
      query = "max_over_time(cpu_usage_user{hostname=\"host_0\"}[1h])"
      {:ok, plan} = PromQL.parse(query)
      assert plan.metric == "cpu_usage_user"
      assert plan.inner_function == :max_over_time
      assert plan.range_duration == 3600
      assert plan.labels == %{"hostname" => "host_0"}
    end

    test "avg_over_time with 5m range" do
      query = "avg_over_time(cpu_usage_user{hostname=\"host_0\"}[5m])"
      {:ok, plan} = PromQL.parse(query)
      assert plan.inner_function == :avg_over_time
      assert plan.range_duration == 300
    end

    test "rate function" do
      query = "rate(cpu_usage_user{hostname=\"host_0\"}[1m])"
      {:ok, plan} = PromQL.parse(query)
      assert plan.inner_function == :rate
      assert plan.range_duration == 60
    end
  end

  describe "parse/1 - outer aggregation + group-by" do
    test "max with group by hostname" do
      query = "max(max_over_time(cpu_usage_user{hostname=~\"host_0|host_1\"}[1h])) by (hostname)"
      {:ok, plan} = PromQL.parse(query)

      assert plan.metric == "cpu_usage_user"
      assert plan.outer_aggregate == :max
      assert plan.inner_function == :max_over_time
      assert plan.group_by == ["hostname"]
      assert plan.labels == %{"hostname" => {:regex, "host_0|host_1"}}
    end

    test "avg with multiple group-by keys" do
      query = "avg(avg_over_time(cpu_usage_user{hostname=~\"host_0\"}[1h])) by (hostname, region)"
      {:ok, plan} = PromQL.parse(query)

      assert plan.outer_aggregate == :avg
      assert plan.group_by == ["hostname", "region"]
    end
  end

  describe "parse/1 - threshold" do
    test "greater than threshold" do
      query = "max(max_over_time(cpu_usage_user{hostname=~\"host_0\"}[1h])) by (hostname) > 90"
      {:ok, plan} = PromQL.parse(query)

      assert plan.threshold == {:gt, 90.0}
      assert plan.outer_aggregate == :max
      assert plan.group_by == ["hostname"]
    end

    test "less than threshold" do
      query = "max(max_over_time(cpu_usage_user{hostname=\"host_0\"}[1h])) by (hostname) < 10.5"
      {:ok, plan} = PromQL.parse(query)

      assert plan.threshold == {:lt, 10.5}
    end
  end

  describe "parse/1 - multi-metric via __name__" do
    test "__name__ regex pattern" do
      query = "max(max_over_time({__name__=~\"cpu_.*\",hostname=~\"host_0\"}[1h])) by (hostname)"
      {:ok, plan} = PromQL.parse(query)

      assert plan.metric == nil
      assert plan.metric_pattern == "cpu_.*"
      assert plan.labels == %{"hostname" => {:regex, "host_0"}}
      assert plan.outer_aggregate == :max
      assert plan.group_by == ["hostname"]
    end
  end

  describe "parse/1 - single-quoted labels" do
    test "exact match with single quotes" do
      {:ok, plan} = PromQL.parse("cpu_usage_user{hostname='host_0'}")
      assert plan.metric == "cpu_usage_user"
      assert plan.labels == %{"hostname" => "host_0"}
    end

    test "regex match with single quotes" do
      {:ok, plan} = PromQL.parse("cpu_usage_user{hostname=~'host_0|host_1'}")
      assert plan.labels == %{"hostname" => {:regex, "host_0|host_1"}}
    end

    test "mixed single and double quotes" do
      {:ok, plan} = PromQL.parse("cpu{hostname='host_0',region=\"us-east\"}")
      assert plan.labels == %{"hostname" => "host_0", "region" => "us-east"}
    end

    test "empty label selector" do
      {:ok, plan} = PromQL.parse("cpu_usage_user{}")
      assert plan.metric == "cpu_usage_user"
      assert plan.labels == %{}
    end

    test "spaces after commas in labels" do
      {:ok, plan} = PromQL.parse("cpu{hostname='host_0', region='us-east'}")
      assert plan.labels == %{"hostname" => "host_0", "region" => "us-east"}
    end
  end

  describe "parse/1 - TSBS DevOps query patterns" do
    test "SingleGroupby: 1-host-1-hr" do
      query = "max(max_over_time(cpu_usage_user{hostname=~\"host_0\"}[1h])) by (hostname)"
      {:ok, plan} = PromQL.parse(query)
      assert plan.metric == "cpu_usage_user"
      assert plan.labels == %{"hostname" => {:regex, "host_0"}}
      assert plan.outer_aggregate == :max
      assert plan.inner_function == :max_over_time
      assert plan.group_by == ["hostname"]
    end

    test "SingleGroupby-5: TSBS actual format with single quotes and grouped regex" do
      query = "max(max_over_time({__name__=~'cpu_(usage_user|usage_system|usage_idle|usage_nice|usage_iowait)', hostname='host_35'}[1m])) by (__name__)"
      {:ok, plan} = PromQL.parse(query)
      assert plan.metric_pattern == "cpu_(usage_user|usage_system|usage_idle|usage_nice|usage_iowait)"
      assert plan.labels == %{"hostname" => "host_35"}
      assert plan.group_by == ["__name__"]
    end

    test "DoubleGroupby: 5-metrics-1-host-1-hr" do
      query = "max(max_over_time({__name__=~\"cpu_usage_user|cpu_usage_system|cpu_usage_idle|cpu_usage_nice|cpu_usage_iowait\",hostname=~\"host_0\"}[1h])) by (hostname)"
      {:ok, plan} = PromQL.parse(query)
      assert plan.metric_pattern ==
               "cpu_usage_user|cpu_usage_system|cpu_usage_idle|cpu_usage_nice|cpu_usage_iowait"

      assert plan.group_by == ["hostname"]
    end

    test "MaxAllCPU: all-cpu-metrics-8-hosts-1-hr" do
      query = "max(max_over_time({__name__=~\"cpu_.*\",hostname=~\"host_0|host_1|host_2|host_3|host_4|host_5|host_6|host_7\"}[1h])) by (hostname)"
      {:ok, plan} = PromQL.parse(query)
      assert plan.metric_pattern == "cpu_.*"
      assert plan.outer_aggregate == :max
      assert plan.group_by == ["hostname"]
    end

    test "HighCPU: cpu > threshold" do
      query = "max(max_over_time(cpu_usage_user{hostname=~\"host_.*\"}[1h])) by (hostname) > 90"
      {:ok, plan} = PromQL.parse(query)
      assert plan.metric == "cpu_usage_user"
      assert plan.threshold == {:gt, 90.0}
      assert plan.group_by == ["hostname"]
    end
  end
end
