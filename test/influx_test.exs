defmodule TimelessMetrics.InfluxTest do
  use ExUnit.Case, async: false

  @moduledoc """
  Tests for InfluxDB line protocol ingestion and TSBS query routing.
  """

  @data_dir "/tmp/timeless_influx_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {TimelessMetrics, name: :influx_test, data_dir: @data_dir, engine: :actor}
    )

    on_exit(fn -> File.rm_rf!(@data_dir) end)

    :ok
  end

  describe "POST /write - InfluxDB line protocol" do
    test "ingests single measurement with tags and single field" do
      # cpu,hostname=host_0,region=us-east usage_user=73.2 1700000000000000000
      body = "cpu,hostname=host_0,region=us-east usage_user=73.2 1700000000000000000\n"

      conn =
        Plug.Test.conn(:post, "/write", body)
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 204

      TimelessMetrics.flush(:influx_test)

      {:ok, results} =
        TimelessMetrics.query_multi(:influx_test, "cpu_usage_user", %{"hostname" => "host_0"},
          from: 1_699_999_999,
          to: 1_700_000_001
        )

      assert length(results) == 1
      assert hd(results).labels["hostname"] == "host_0"
      assert hd(results).labels["region"] == "us-east"

      [{ts, val}] = hd(results).points
      assert ts == 1_700_000_000
      assert_in_delta val, 73.2, 0.1
    end

    test "ingests multi-field measurement — each field becomes a metric" do
      body = "cpu,hostname=host_0 usage_user=73.2,usage_system=12.5 1700000000000000000\n"

      conn =
        Plug.Test.conn(:post, "/write", body)
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 204
      TimelessMetrics.flush(:influx_test)

      {:ok, user_results} =
        TimelessMetrics.query_multi(:influx_test, "cpu_usage_user", %{},
          from: 1_699_999_999,
          to: 1_700_000_001
        )

      {:ok, sys_results} =
        TimelessMetrics.query_multi(:influx_test, "cpu_usage_system", %{},
          from: 1_699_999_999,
          to: 1_700_000_001
        )

      assert length(user_results) == 1
      assert length(sys_results) == 1

      [{_, user_val}] = hd(user_results).points
      [{_, sys_val}] = hd(sys_results).points
      assert_in_delta user_val, 73.2, 0.1
      assert_in_delta sys_val, 12.5, 0.1
    end

    test "ingests single field named 'value' — uses measurement as metric" do
      body = "temperature,sensor=t1 value=22.5 1700000000000000000\n"

      conn =
        Plug.Test.conn(:post, "/write", body)
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 204
      TimelessMetrics.flush(:influx_test)

      {:ok, results} =
        TimelessMetrics.query_multi(:influx_test, "temperature", %{},
          from: 1_699_999_999,
          to: 1_700_000_001
        )

      assert length(results) == 1
    end

    test "handles batch of lines" do
      lines =
        for i <- 0..9 do
          ts = 1_700_000_000 + i * 10
          ts_ns = ts * 1_000_000_000
          "cpu,hostname=host_#{i} usage_user=#{50.0 + i} #{ts_ns}"
        end

      body = Enum.join(lines, "\n") <> "\n"

      conn =
        Plug.Test.conn(:post, "/write", body)
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 204
      TimelessMetrics.flush(:influx_test)

      {:ok, results} =
        TimelessMetrics.query_multi(:influx_test, "cpu_usage_user", %{},
          from: 1_699_999_999,
          to: 1_700_000_200
        )

      assert length(results) == 10
    end

    test "handles integer field values with 'i' suffix" do
      body = "mem,hostname=host_0 total=8589934592i 1700000000000000000\n"

      conn =
        Plug.Test.conn(:post, "/write", body)
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 204
      TimelessMetrics.flush(:influx_test)

      {:ok, results} =
        TimelessMetrics.query_multi(:influx_test, "mem_total", %{},
          from: 1_699_999_999,
          to: 1_700_000_001
        )

      assert length(results) == 1
      [{_, val}] = hd(results).points
      assert_in_delta val, 8_589_934_592.0, 1.0
    end

    test "skips string and boolean field values" do
      body = "status,host=h1 message=\"ok\",healthy=true,code=200i 1700000000000000000\n"

      conn =
        Plug.Test.conn(:post, "/write", body)
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 204
      TimelessMetrics.flush(:influx_test)

      # Only code=200 should be ingested (string and bool skipped)
      {:ok, results} =
        TimelessMetrics.query_multi(:influx_test, "status_code", %{},
          from: 1_699_999_999,
          to: 1_700_000_001
        )

      assert length(results) == 1
      [{_, val}] = hd(results).points
      assert_in_delta val, 200.0, 0.1

      # message and healthy should not create metrics
      {:ok, msg_results} =
        TimelessMetrics.query_multi(:influx_test, "status_message", %{},
          from: 1_699_999_999,
          to: 1_700_000_001
        )

      assert msg_results == []
    end

    test "handles no tags" do
      body = "uptime value=12345.0 1700000000000000000\n"

      conn =
        Plug.Test.conn(:post, "/write", body)
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 204
      TimelessMetrics.flush(:influx_test)

      {:ok, results} =
        TimelessMetrics.query_multi(:influx_test, "uptime", %{},
          from: 1_699_999_999,
          to: 1_700_000_001
        )

      assert length(results) == 1
      assert hd(results).labels == %{}
    end

    test "handles no timestamp — uses current time" do
      body = "cpu,hostname=host_0 usage_user=50.0\n"

      conn =
        Plug.Test.conn(:post, "/write", body)
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 204
      TimelessMetrics.flush(:influx_test)

      now = System.os_time(:second)

      {:ok, results} =
        TimelessMetrics.query_multi(:influx_test, "cpu_usage_user", %{},
          from: now - 10,
          to: now + 10
        )

      assert length(results) == 1
    end

    test "handles millisecond timestamps" do
      body = "cpu,hostname=host_0 usage_user=50.0 1700000000000\n"

      conn =
        Plug.Test.conn(:post, "/write", body)
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 204
      TimelessMetrics.flush(:influx_test)

      {:ok, results} =
        TimelessMetrics.query_multi(:influx_test, "cpu_usage_user", %{},
          from: 1_699_999_999,
          to: 1_700_000_001
        )

      assert length(results) == 1
      [{ts, _}] = hd(results).points
      assert ts == 1_700_000_000
    end

    test "skips comment and empty lines" do
      body = """
      # This is a comment
      cpu,hostname=host_0 usage_user=50.0 1700000000000000000

      # Another comment
      cpu,hostname=host_1 usage_user=60.0 1700000000000000000
      """

      conn =
        Plug.Test.conn(:post, "/write", body)
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 204
      TimelessMetrics.flush(:influx_test)

      {:ok, results} =
        TimelessMetrics.query_multi(:influx_test, "cpu_usage_user", %{},
          from: 1_699_999_999,
          to: 1_700_000_001
        )

      assert length(results) == 2
    end

    test "reports partial failures" do
      body = "cpu,hostname=host_0 usage_user=50.0 1700000000000000000\nnot_valid_at_all\n"

      conn =
        Plug.Test.conn(:post, "/write", body)
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      # Partial success returns 200 with error info
      assert conn.status == 200
      resp = Jason.decode!(conn.resp_body)
      assert resp["samples"] == 1
      assert resp["errors"] == 1
    end
  end

  describe "GET /api/v1/query_range with query= param (TSBS routing)" do
    setup do
      # Write test data
      now = 1_700_000_000

      for host_idx <- 0..2 do
        for i <- 0..9 do
          TimelessMetrics.write(
            :influx_test,
            "cpu_usage_user",
            %{"hostname" => "host_#{host_idx}"},
            host_idx * 20.0 + i * 1.0,
            timestamp: now + i * 10
          )
        end
      end

      TimelessMetrics.flush(:influx_test)
      {:ok, base_ts: now}
    end

    test "routes to PromQL parser when query= present", %{base_ts: ts} do
      query = URI.encode("cpu_usage_user{hostname=\"host_0\"}")

      conn =
        Plug.Test.conn(
          :get,
          "/api/v1/query_range?query=#{query}&start=#{ts}&end=#{ts + 100}&step=100"
        )
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body["status"] == "success"
      assert body["data"]["resultType"] == "matrix"
      assert length(body["data"]["result"]) == 1

      result = hd(body["data"]["result"])
      assert result["metric"]["hostname"] == "host_0"
      assert result["metric"]["__name__"] == "cpu_usage_user"
    end

    test "still uses native params when query= absent", %{base_ts: ts} do
      conn =
        Plug.Test.conn(
          :get,
          "/api/v1/query_range?metric=cpu_usage_user&hostname=host_0&start=#{ts}&end=#{ts + 100}&step=100"
        )
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      # Native response format (not Prometheus format)
      assert body["metric"] == "cpu_usage_user"
      assert length(body["series"]) == 1
    end

    test "handles PromQL with group-by on /api/v1/query_range", %{base_ts: ts} do
      query =
        URI.encode(
          "max(max_over_time(cpu_usage_user{hostname=~\"host_0|host_1|host_2\"}[1h])) by (hostname)"
        )

      conn =
        Plug.Test.conn(
          :get,
          "/api/v1/query_range?query=#{query}&start=#{ts}&end=#{ts + 100}&step=100"
        )
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body["status"] == "success"
      assert length(body["data"]["result"]) == 3

      hostnames =
        Enum.map(body["data"]["result"], & &1["metric"]["hostname"]) |> Enum.sort()

      assert hostnames == ["host_0", "host_1", "host_2"]
    end
  end

  describe "TSBS-style end-to-end: load via /write, query via /api/v1/query_range" do
    test "full TSBS workflow simulation" do
      ts_base = 1_700_000_000

      # 1. Load data via InfluxDB line protocol (what tsbs_load does)
      lines =
        for host_idx <- 0..4, i <- 0..9 do
          ts_ns = (ts_base + i * 60) * 1_000_000_000
          val = host_idx * 15.0 + i * 1.0

          "cpu,hostname=host_#{host_idx},region=us-east usage_user=#{val},usage_system=#{val / 2.0} #{ts_ns}"
        end

      body = Enum.join(lines, "\n") <> "\n"

      load_conn =
        Plug.Test.conn(:post, "/write", body)
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert load_conn.status == 204

      TimelessMetrics.flush(:influx_test)

      # 2. Query via PromQL on /api/v1/query_range (what tsbs_run_queries does)
      # GroupByTime pattern: max of all cpu metrics grouped by __name__
      query =
        URI.encode(
          "max(max_over_time(cpu_usage_user{hostname=~\"host_0|host_1|host_2|host_3|host_4\"}[1h])) by (hostname)"
        )

      query_conn =
        Plug.Test.conn(
          :get,
          "/api/v1/query_range?query=#{query}&start=#{ts_base}&end=#{ts_base + 600}&step=600"
        )
        |> TimelessMetrics.HTTP.call(store: :influx_test)

      assert query_conn.status == 200
      body = Jason.decode!(query_conn.resp_body)
      assert body["status"] == "success"
      assert length(body["data"]["result"]) == 5

      # Verify results are grouped by hostname with correct values
      hostnames =
        Enum.map(body["data"]["result"], & &1["metric"]["hostname"]) |> Enum.sort()

      assert hostnames == ["host_0", "host_1", "host_2", "host_3", "host_4"]
    end
  end
end
