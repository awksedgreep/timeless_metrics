defmodule TimelessMetrics.TSBSDevOpsTest do
  use ExUnit.Case, async: false

  @moduledoc """
  End-to-end simulation of TSBS DevOps query types.

  Writes synthetic data matching TSBS patterns (10 hosts x 10 CPU metrics x 100 points),
  then runs all 6 DevOps query types and verifies correct results.
  """

  @data_dir "/tmp/timeless_tsbs_test_#{System.os_time(:millisecond)}"
  @hosts 10
  @cpu_metrics ~w(
    cpu_usage_user cpu_usage_system cpu_usage_idle cpu_usage_nice
    cpu_usage_iowait cpu_usage_irq cpu_usage_softirq cpu_usage_steal
    cpu_usage_guest cpu_usage_guest_nice
  )
  @points_per_series 100
  @base_ts 1_700_000_000

  setup do
    start_supervised!(
      {TimelessMetrics, name: :tsbs_test, data_dir: @data_dir, engine: :actor}
    )

    # Write TSBS-style data: 10 hosts x 10 CPU metrics x 100 points
    entries =
      for host_idx <- 0..(@hosts - 1),
          metric <- @cpu_metrics,
          point_idx <- 0..(@points_per_series - 1) do
        hostname = "host_#{host_idx}"
        ts = @base_ts + point_idx * 10
        # Each host has a distinct baseline so we can verify group-by
        value = host_idx * 10.0 + :rand.uniform() * 5.0

        {metric, %{"hostname" => hostname}, value, ts}
      end

    TimelessMetrics.write_batch(:tsbs_test, entries)
    TimelessMetrics.flush(:tsbs_test)

    on_exit(fn ->

      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  describe "Phase 1: Regex label matching" do
    test "exact label match returns one host" do
      {:ok, results} =
        TimelessMetrics.query_aggregate_multi(:tsbs_test, "cpu_usage_user", %{"hostname" => "host_0"},
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :avg
        )

      assert length(results) == 1
      assert hd(results).labels["hostname"] == "host_0"
    end

    test "regex label match returns multiple hosts" do
      {:ok, results} =
        TimelessMetrics.query_aggregate_multi(
          :tsbs_test,
          "cpu_usage_user",
          %{"hostname" => {:regex, "host_0|host_1|host_2"}},
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :avg
        )

      assert length(results) == 3
      hosts = Enum.map(results, & &1.labels["hostname"]) |> Enum.sort()
      assert hosts == ["host_0", "host_1", "host_2"]
    end

    test "regex with wildcard matches all hosts" do
      {:ok, results} =
        TimelessMetrics.query_aggregate_multi(
          :tsbs_test,
          "cpu_usage_user",
          %{"hostname" => {:regex, "host_.*"}},
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :avg
        )

      assert length(results) == @hosts
    end

    test "backwards compat: plain string still does exact match" do
      {:ok, results} =
        TimelessMetrics.query_aggregate_multi(
          :tsbs_test,
          "cpu_usage_user",
          %{"hostname" => "host_5"},
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :avg
        )

      assert length(results) == 1
      assert hd(results).labels["hostname"] == "host_5"
    end
  end

  describe "Phase 1: Multi-metric queries" do
    test "query_aggregate_multi_metrics returns results tagged with metric name" do
      {:ok, results} =
        TimelessMetrics.query_aggregate_multi_metrics(
          :tsbs_test,
          ["cpu_usage_user", "cpu_usage_system"],
          %{"hostname" => "host_0"},
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :avg
        )

      assert length(results) == 2
      metrics = Enum.map(results, & &1.metric) |> Enum.sort()
      assert metrics == ["cpu_usage_system", "cpu_usage_user"]
    end

    test "multi-metric with regex labels" do
      {:ok, results} =
        TimelessMetrics.query_aggregate_multi_metrics(
          :tsbs_test,
          ["cpu_usage_user", "cpu_usage_system"],
          %{"hostname" => {:regex, "host_0|host_1"}},
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :avg
        )

      # 2 metrics x 2 hosts = 4 results
      assert length(results) == 4
    end
  end

  describe "Phase 2: TSBS SingleGroupby" do
    test "max cpu_usage_user grouped by hostname for 1 host" do
      {:ok, grouped} =
        TimelessMetrics.query_aggregate_grouped(
          :tsbs_test,
          "cpu_usage_user",
          %{"hostname" => {:regex, "host_0"}},
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :max,
          group_by: "hostname",
          cross_series_aggregate: :max
        )

      assert length(grouped) == 1
      assert hd(grouped).group == %{"hostname" => "host_0"}
      assert hd(grouped).data != []
    end

    test "max cpu_usage_user grouped by hostname for multiple hosts" do
      {:ok, grouped} =
        TimelessMetrics.query_aggregate_grouped(
          :tsbs_test,
          "cpu_usage_user",
          %{"hostname" => {:regex, "host_0|host_1|host_2|host_3"}},
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :max,
          group_by: "hostname",
          cross_series_aggregate: :max
        )

      assert length(grouped) == 4
      group_hosts = Enum.map(grouped, & &1.group["hostname"]) |> Enum.sort()
      assert group_hosts == ["host_0", "host_1", "host_2", "host_3"]
    end
  end

  describe "Phase 2: TSBS DoubleGroupby" do
    test "5 metrics grouped by hostname" do
      metrics = Enum.take(@cpu_metrics, 5)

      {:ok, grouped} =
        TimelessMetrics.query_aggregate_grouped_metrics(
          :tsbs_test,
          metrics,
          %{"hostname" => {:regex, "host_0"}},
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :max,
          group_by: "hostname",
          cross_series_aggregate: :max
        )

      assert length(grouped) == 1
      assert hd(grouped).group == %{"hostname" => "host_0"}
      assert hd(grouped).data != []
    end
  end

  describe "Phase 2: TSBS MaxAllCPU" do
    test "all CPU metrics grouped by hostname across 8 hosts" do
      {:ok, grouped} =
        TimelessMetrics.query_aggregate_grouped_metrics(
          :tsbs_test,
          @cpu_metrics,
          %{
            "hostname" =>
              {:regex, "host_0|host_1|host_2|host_3|host_4|host_5|host_6|host_7"}
          },
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :max,
          group_by: "hostname",
          cross_series_aggregate: :max
        )

      assert length(grouped) == 8
      group_hosts = Enum.map(grouped, & &1.group["hostname"]) |> Enum.sort()

      assert group_hosts ==
               Enum.map(0..7, &"host_#{&1}")
    end
  end

  describe "Phase 3: TSBS HighCPU" do
    test "filter series exceeding threshold" do
      {:ok, results} =
        TimelessMetrics.query_aggregate_multi_filtered(
          :tsbs_test,
          "cpu_usage_user",
          %{"hostname" => {:regex, "host_.*"}},
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :max,
          threshold: {:gt, 50.0}
        )

      # Hosts with baseline >= 50 should match (host_5 through host_9 have baselines 50-90)
      hosts = Enum.map(results, & &1.labels["hostname"]) |> Enum.sort()
      assert length(hosts) > 0

      # All returned series should have at least one value > 50
      Enum.each(results, fn %{data: data} ->
        assert Enum.any?(data, fn {_ts, val} -> val > 50.0 end)
      end)
    end

    test "threshold removes series below threshold" do
      {:ok, results} =
        TimelessMetrics.query_aggregate_multi_filtered(
          :tsbs_test,
          "cpu_usage_user",
          %{"hostname" => {:regex, "host_.*"}},
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :max,
          threshold: {:gt, 200.0}
        )

      # No host has values > 200 (max is ~95)
      assert results == []
    end
  end

  describe "Phase 3: Top-N" do
    test "top_n returns N highest-value series" do
      {:ok, results} =
        TimelessMetrics.query_aggregate_multi(
          :tsbs_test,
          "cpu_usage_user",
          %{"hostname" => {:regex, "host_.*"}},
          from: @base_ts,
          to: @base_ts + 1000,
          bucket: {1000, :seconds},
          aggregate: :avg
        )

      top_3 = TimelessMetrics.top_n(results, 3)
      assert length(top_3) == 3

      # Values should be in descending order
      values = Enum.map(top_3, fn %{data: data} -> data |> List.last() |> elem(1) end)
      assert values == Enum.sort(values, :desc)
    end
  end

  describe "Phase 4: PromQL parse + execute" do
    test "simple selector via PromQL" do
      {:ok, plan} =
        TimelessMetrics.PromQL.parse("cpu_usage_user{hostname=\"host_0\"}")

      {:ok, response} =
        TimelessMetrics.PromQL.execute(
          plan,
          :tsbs_test,
          @base_ts,
          @base_ts + 1000,
          1000
        )

      assert response["status"] == "success"
      assert response["data"]["resultType"] == "matrix"
      assert length(response["data"]["result"]) == 1

      result = hd(response["data"]["result"])
      assert result["metric"]["__name__"] == "cpu_usage_user"
      assert result["metric"]["hostname"] == "host_0"
    end

    test "SingleGroupby pattern via PromQL" do
      {:ok, plan} =
        TimelessMetrics.PromQL.parse(
          "max(max_over_time(cpu_usage_user{hostname=~\"host_0|host_1\"}[1h])) by (hostname)"
        )

      {:ok, response} =
        TimelessMetrics.PromQL.execute(
          plan,
          :tsbs_test,
          @base_ts,
          @base_ts + 1000,
          1000
        )

      assert response["status"] == "success"
      assert length(response["data"]["result"]) == 2

      hostnames =
        Enum.map(response["data"]["result"], & &1["metric"]["hostname"]) |> Enum.sort()

      assert hostnames == ["host_0", "host_1"]
    end

    test "HighCPU pattern via PromQL (with threshold)" do
      {:ok, plan} =
        TimelessMetrics.PromQL.parse(
          "max(max_over_time(cpu_usage_user{hostname=~\"host_.*\"}[1h])) by (hostname) > 50"
        )

      {:ok, response} =
        TimelessMetrics.PromQL.execute(
          plan,
          :tsbs_test,
          @base_ts,
          @base_ts + 1000,
          1000
        )

      assert response["status"] == "success"
      # Only hosts with max CPU > 50 should appear
      assert length(response["data"]["result"]) > 0

      Enum.each(response["data"]["result"], fn result ->
        values =
          Enum.map(result["values"], fn [_ts, val_str] ->
            {f, _} = Float.parse(val_str)
            f
          end)

        assert Enum.any?(values, &(&1 > 50.0))
      end)
    end

    test "multi-metric via __name__ regex" do
      {:ok, plan} =
        TimelessMetrics.PromQL.parse(
          "max(max_over_time({__name__=~\"cpu_usage_user|cpu_usage_system\",hostname=~\"host_0\"}[1h])) by (hostname)"
        )

      {:ok, response} =
        TimelessMetrics.PromQL.execute(
          plan,
          :tsbs_test,
          @base_ts,
          @base_ts + 1000,
          1000
        )

      assert response["status"] == "success"
      # Grouped by hostname — should be 1 group (host_0) with merged CPU data
      assert length(response["data"]["result"]) == 1
      assert hd(response["data"]["result"])["metric"]["hostname"] == "host_0"
    end
  end

  describe "Phase 5: Bulk lastpoint" do
    test "latest_multi returns latest values for all matching series" do
      {:ok, results} = TimelessMetrics.latest_multi(:tsbs_test, "cpu_usage_user")

      assert length(results) == @hosts

      Enum.each(results, fn %{labels: labels, timestamp: ts, value: val} ->
        assert labels["hostname"] != nil
        assert is_integer(ts)
        assert is_number(val)
      end)
    end

    test "latest_multi with label filter" do
      {:ok, results} =
        TimelessMetrics.latest_multi(
          :tsbs_test,
          "cpu_usage_user",
          %{"hostname" => {:regex, "host_0|host_1"}}
        )

      assert length(results) == 2
      hosts = Enum.map(results, & &1.labels["hostname"]) |> Enum.sort()
      assert hosts == ["host_0", "host_1"]
    end
  end

  describe "Phase 4: HTTP Prometheus endpoint" do
    test "PromQL query via HTTP endpoint" do
      conn =
        Plug.Test.conn(
          :get,
          "/prometheus/api/v1/query_range?query=cpu_usage_user%7Bhostname%3D%22host_0%22%7D&start=#{@base_ts}&end=#{@base_ts + 1000}&step=1000"
        )
        |> TimelessMetrics.HTTP.call(store: :tsbs_test)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body["status"] == "success"
      assert body["data"]["resultType"] == "matrix"
      assert length(body["data"]["result"]) == 1
    end
  end

  describe "merge_series_data/2" do
    test "merges multiple series by timestamp with max" do
      s1 = [{100, 10.0}, {200, 20.0}, {300, 30.0}]
      s2 = [{100, 15.0}, {200, 12.0}, {300, 35.0}]

      merged = TimelessMetrics.merge_series_data([s1, s2], :max)
      assert merged == [{100, 15.0}, {200, 20.0}, {300, 35.0}]
    end

    test "merges with avg" do
      s1 = [{100, 10.0}, {200, 20.0}]
      s2 = [{100, 30.0}, {200, 40.0}]

      merged = TimelessMetrics.merge_series_data([s1, s2], :avg)
      assert merged == [{100, 20.0}, {200, 30.0}]
    end

    test "merges with sum" do
      s1 = [{100, 10.0}]
      s2 = [{100, 20.0}]
      s3 = [{100, 30.0}]

      merged = TimelessMetrics.merge_series_data([s1, s2, s3], :sum)
      assert merged == [{100, 60.0}]
    end
  end
end
