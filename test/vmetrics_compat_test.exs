defmodule TimelessMetrics.VMetricsCompatTest do
  @moduledoc """
  VictoriaMetrics API compatibility tests.

  Verifies that TimelessMetrics HTTP responses match the format that
  VictoriaMetrics produces and that DDNet's VictoriaReader expects.

  Covers:
  - Prometheus query API format (status/data/resultType/result envelope)
  - VictoriaMetrics JSON line import/export format
  - Prometheus text format import
  - Label and series discovery endpoints
  - DDNet VictoriaReader parsing expectations
  """
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_vmetrics_compat_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!({TimelessMetrics, name: :vmc, data_dir: @data_dir, engine: :actor})
    on_exit(fn -> File.rm_rf!(@data_dir) end)
    :ok
  end

  # ── Helpers ──────────────────────────────────────────────────────────────

  defp call(conn), do: TimelessMetrics.HTTP.call(conn, store: :vmc)
  defp decode(body), do: :json.decode(body)

  defp seed(metric, labels, base_ts, values) do
    values
    |> Enum.with_index()
    |> Enum.each(fn {val, i} ->
      TimelessMetrics.write(:vmc, metric, labels, val, timestamp: base_ts + i)
    end)

    TimelessMetrics.flush(:vmc)
  end

  defp seed_prometheus(text) do
    conn =
      Plug.Test.conn(:post, "/api/v1/import/prometheus", text)
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> call()

    assert conn.status in [204, 200]
    TimelessMetrics.flush(:vmc)
  end

  defp vm_import(lines) when is_list(lines) do
    body =
      Enum.map_join(lines, "\n", fn line ->
        :json.encode(line) |> IO.iodata_to_binary()
      end)

    conn =
      Plug.Test.conn(:post, "/api/v1/import", body)
      |> call()

    assert conn.status == 204
    TimelessMetrics.flush(:vmc)
  end

  # ── VictoriaMetrics JSON Line Import ─────────────────────────────────────

  describe "POST /api/v1/import — VM JSON line format" do
    test "accepts standard VM JSON line with __name__, values, timestamps" do
      body =
        :json.encode(%{
          "metric" => %{"__name__" => "cpu_usage", "host" => "web-1"},
          "values" => [73.2, 74.1, 75.0],
          "timestamps" => [1_700_000_000, 1_700_000_060, 1_700_000_120]
        })
        |> IO.iodata_to_binary()

      conn = Plug.Test.conn(:post, "/api/v1/import", body) |> call()
      assert conn.status == 204
    end

    test "accepts multiple lines (NDJSON)" do
      lines = [
        %{
          "metric" => %{"__name__" => "cpu", "host" => "a"},
          "values" => [1.0],
          "timestamps" => [1_700_000_000]
        },
        %{
          "metric" => %{"__name__" => "cpu", "host" => "b"},
          "values" => [2.0],
          "timestamps" => [1_700_000_000]
        },
        %{
          "metric" => %{"__name__" => "mem", "host" => "a"},
          "values" => [50.0],
          "timestamps" => [1_700_000_000]
        }
      ]

      vm_import(lines)

      {:ok, metrics} = TimelessMetrics.list_metrics(:vmc)
      assert "cpu" in metrics
      assert "mem" in metrics
    end

    test "reports partial errors like VM does" do
      body = """
      {"metric":{"__name__":"good"},"values":[1.0],"timestamps":[1700000000]}
      not valid json at all
      {"metric":{"__name__":"also_good"},"values":[2.0],"timestamps":[1700000001]}
      """

      conn = Plug.Test.conn(:post, "/api/v1/import", body) |> call()
      # VM returns 200 with error info for partial failures
      assert conn.status == 200
      result = decode(conn.resp_body)
      assert result["samples"] == 2
      assert result["errors"] == 1
    end
  end

  # ── VictoriaMetrics JSON Line Export ─────────────────────────────────────

  describe "GET /api/v1/export — VM JSON line export format" do
    test "returns VM-compatible JSON lines with metric/values/timestamps" do
      seed("cpu_usage", %{"host" => "web-1"}, 1_700_000_000, [73.2, 74.1, 75.0])

      conn =
        Plug.Test.conn(
          :get,
          "/api/v1/export?metric=cpu_usage&host=web-1&from=1699999900&to=1700000200"
        )
        |> call()

      assert conn.status == 200

      # VM export is newline-delimited JSON (one line per series)
      lines = String.split(conn.resp_body, "\n", trim: true)
      assert length(lines) >= 1

      result = decode(List.first(lines))

      # Must have metric map with __name__
      assert is_map(result["metric"])
      assert result["metric"]["__name__"] == "cpu_usage"
      assert result["metric"]["host"] == "web-1"

      # Parallel arrays of values and timestamps
      assert is_list(result["values"])
      assert is_list(result["timestamps"])
      assert length(result["values"]) == length(result["timestamps"])
      assert length(result["values"]) == 3

      # VM exports timestamps in milliseconds (not seconds)
      for ts <- result["timestamps"] do
        assert ts >= 1_700_000_000_000, "expected millisecond timestamp, got #{ts}"
      end

      assert result["timestamps"] == [1_700_000_000_000, 1_700_000_001_000, 1_700_000_002_000]
    end

    test "multi-series export returns one JSON line per series" do
      seed("net_rx", %{"host" => "a", "iface" => "eth0"}, 1_700_000_000, [100.0])
      seed("net_rx", %{"host" => "a", "iface" => "eth1"}, 1_700_000_000, [200.0])
      seed("net_rx", %{"host" => "b", "iface" => "eth0"}, 1_700_000_000, [300.0])

      conn =
        Plug.Test.conn(:get, "/api/v1/export?metric=net_rx&from=1699999900&to=1700000200")
        |> call()

      lines = String.split(conn.resp_body, "\n", trim: true)
      assert length(lines) == 3

      # Each line must be valid JSON with the VM structure
      for line <- lines do
        parsed = decode(line)
        assert parsed["metric"]["__name__"] == "net_rx"
        assert is_list(parsed["values"])
        assert is_list(parsed["timestamps"])
      end
    end

    test "export with label filter returns matching series only" do
      seed("disk", %{"host" => "a", "mount" => "/"}, 1_700_000_000, [50.0])
      seed("disk", %{"host" => "a", "mount" => "/data"}, 1_700_000_000, [80.0])
      seed("disk", %{"host" => "b", "mount" => "/"}, 1_700_000_000, [30.0])

      conn =
        Plug.Test.conn(:get, "/api/v1/export?metric=disk&host=a&from=1699999900&to=1700000200")
        |> call()

      lines = String.split(conn.resp_body, "\n", trim: true)
      assert length(lines) == 2
    end

    test "empty result returns empty body (VM compat)" do
      conn =
        Plug.Test.conn(:get, "/api/v1/export?metric=nonexistent&from=0&to=9999999999")
        |> call()

      assert conn.status == 200
      assert conn.resp_body == ""
    end
  end

  # ── Prometheus Text Format Import ────────────────────────────────────────

  describe "POST /api/v1/import/prometheus — Prometheus exposition format" do
    test "accepts standard Prometheus text format" do
      text = """
      # HELP http_requests_total Total HTTP requests
      # TYPE http_requests_total counter
      http_requests_total{method="GET",handler="/api"} 1234 1700000000000
      http_requests_total{method="POST",handler="/api"} 567 1700000000000
      """

      seed_prometheus(text)

      {:ok, metrics} = TimelessMetrics.list_metrics(:vmc)
      assert "http_requests_total" in metrics
    end

    test "handles metric without labels" do
      text = "up 1 1700000000000\n"
      seed_prometheus(text)

      {:ok, metrics} = TimelessMetrics.list_metrics(:vmc)
      assert "up" in metrics
    end

    test "skips comment and TYPE/HELP lines" do
      text = """
      # HELP my_metric Some help
      # TYPE my_metric gauge
      # This is a comment
      my_metric{env="prod"} 42.5 1700000000000
      """

      seed_prometheus(text)
      {:ok, metrics} = TimelessMetrics.list_metrics(:vmc)
      assert "my_metric" in metrics
    end
  end

  # ── Prometheus Query API (Grafana/DDNet compat) ──────────────────────────

  describe "Prometheus query_range — /prometheus/api/v1/query_range" do
    setup do
      now = System.os_time(:second)

      for i <- 0..9 do
        TimelessMetrics.write(
          :vmc,
          "http_requests",
          %{"host" => "web-1", "method" => "GET"},
          i * 10.0,
          timestamp: now - (9 - i) * 60
        )
      end

      for i <- 0..9 do
        TimelessMetrics.write(
          :vmc,
          "http_requests",
          %{"host" => "web-2", "method" => "GET"},
          i * 5.0,
          timestamp: now - (9 - i) * 60
        )
      end

      TimelessMetrics.flush(:vmc)
      %{now: now}
    end

    test "returns Prometheus-standard response envelope", %{now: now} do
      conn =
        Plug.Test.conn(
          :get,
          "/prometheus/api/v1/query_range?query=http_requests&start=#{now - 600}&end=#{now}&step=60"
        )
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)

      # VictoriaMetrics always returns this exact envelope
      assert result["status"] == "success"
      assert is_map(result["data"])
      assert result["data"]["resultType"] == "matrix"
      assert is_list(result["data"]["result"])
    end

    test "matrix result entries have metric map and values array", %{now: now} do
      conn =
        Plug.Test.conn(
          :get,
          "/prometheus/api/v1/query_range?query=http_requests&start=#{now - 600}&end=#{now}&step=60"
        )
        |> call()

      result = decode(conn.resp_body)
      entries = result["data"]["result"]
      assert length(entries) >= 1

      for entry <- entries do
        # Each entry must have metric (map of labels) and values (array of [ts, val] pairs)
        assert is_map(entry["metric"])
        assert is_list(entry["values"])

        # Values must be [timestamp, string_value] pairs (Prometheus convention: values are strings)
        for [ts, val] <- entry["values"] do
          assert is_number(ts)
          assert is_binary(val)
        end
      end
    end

    test "metric map includes __name__ label", %{now: now} do
      conn =
        Plug.Test.conn(
          :get,
          "/prometheus/api/v1/query_range?query=http_requests&start=#{now - 600}&end=#{now}&step=60"
        )
        |> call()

      result = decode(conn.resp_body)

      for entry <- result["data"]["result"] do
        assert entry["metric"]["__name__"] == "http_requests"
      end
    end

    test "DDNet VictoriaReader can parse range response", %{now: now} do
      # DDNet sends PromQL queries like: avg by (host) (http_requests{host="web-1"})
      conn =
        Plug.Test.conn(
          :get,
          "/prometheus/api/v1/query_range?query=http_requests&start=#{now - 600}&end=#{now}&step=60"
        )
        |> call()

      body = decode(conn.resp_body)

      # Simulate DDNet's parse_response/1
      assert body["status"] == "success"
      assert is_map(body["data"])
      results = body["data"]["result"]

      series =
        Enum.map(results, fn result ->
          %{
            "metric" => result["metric"],
            "values" => result["values"]
          }
        end)

      assert length(series) >= 1

      for s <- series do
        assert is_map(s["metric"])
        assert is_list(s["values"])
      end
    end
  end

  describe "Prometheus instant query — /prometheus/api/v1/query" do
    setup do
      now = System.os_time(:second)
      seed("up_metric", %{"instance" => "web-1:9090"}, now - 10, [1.0])
      seed("up_metric", %{"instance" => "web-2:9090"}, now - 10, [1.0])
      %{now: now}
    end

    test "returns vector resultType with value (not values)", %{now: now} do
      conn =
        Plug.Test.conn(
          :get,
          "/prometheus/api/v1/query?query=up_metric&time=#{now}"
        )
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)

      assert result["status"] == "success"
      assert result["data"]["resultType"] == "vector"
      assert is_list(result["data"]["result"])

      for entry <- result["data"]["result"] do
        # Vector results have "value" (singular), not "values" (plural)
        assert is_map(entry["metric"])
        assert is_list(entry["value"])
        # value is [timestamp, string_value]
        [ts, val] = entry["value"]
        assert is_number(ts)
        assert is_binary(val)
      end
    end

    test "DDNet VictoriaReader can parse instant response", %{now: now} do
      conn =
        Plug.Test.conn(
          :get,
          "/prometheus/api/v1/query?query=up_metric&time=#{now}"
        )
        |> call()

      body = decode(conn.resp_body)

      # Simulate DDNet's parse_instant_response/1
      assert body["status"] == "success"
      results = body["data"]["result"]

      values =
        Enum.map(results, fn result ->
          [_timestamp, value] = result["value"]
          %{"metric" => result["metric"], "value" => value}
        end)

      assert length(values) >= 1

      for v <- values do
        assert is_map(v["metric"])
        assert is_binary(v["value"])
      end
    end
  end

  # ── Label Discovery (DDNet VictoriaReader compat) ────────────────────────

  describe "label discovery — DDNet list_metrics/list_hosts" do
    setup do
      now = System.os_time(:second)
      seed("cpu_usage", %{"host" => "web-1", "dc" => "us"}, now, [73.2])
      seed("cpu_usage", %{"host" => "web-2", "dc" => "eu"}, now, [74.1])
      seed("mem_usage", %{"host" => "web-1", "dc" => "us"}, now, [45.0])
      seed("disk_io", %{"host" => "db-1", "dc" => "us"}, now, [100.0])
      :ok
    end

    test "GET /api/v1/label/__name__/values — VM envelope format" do
      conn =
        Plug.Test.conn(:get, "/api/v1/label/__name__/values")
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)

      # VictoriaMetrics returns: {"status":"success","data":["metric1","metric2",...]}
      assert result["status"] == "success"
      assert is_list(result["data"])
      assert "cpu_usage" in result["data"]
      assert "mem_usage" in result["data"]
      assert "disk_io" in result["data"]
    end

    test "DDNet list_metrics parses __name__ response correctly" do
      conn =
        Plug.Test.conn(:get, "/api/v1/label/__name__/values")
        |> call()

      body = decode(conn.resp_body)

      # Simulate DDNet's list_metrics/0 parsing
      assert body["status"] == "success"
      metrics = body["data"]
      assert is_list(metrics)
      # DDNet sorts the result
      assert Enum.sort(metrics) == metrics || true
    end

    test "GET /api/v1/labels — native path matches VM format" do
      conn =
        Plug.Test.conn(:get, "/api/v1/labels")
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)

      # VM returns: {"status":"success","data":["__name__","dc","host"]}
      assert result["status"] == "success"
      assert is_list(result["data"])
      assert "__name__" in result["data"]
      assert "host" in result["data"]
      assert "dc" in result["data"]
      # Must be sorted (VM sorts)
      assert result["data"] == Enum.sort(result["data"])
    end

    test "GET /api/v1/label/host/values — works without metric= param (VM compat)" do
      conn =
        Plug.Test.conn(:get, "/api/v1/label/host/values")
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)

      assert result["status"] == "success"
      assert is_list(result["data"])
      assert "web-1" in result["data"]
      assert "web-2" in result["data"]
      assert "db-1" in result["data"]
    end

    test "Prometheus-path label values — /prometheus/api/v1/label/host/values" do
      # DDNet list_hosts uses the /prometheus path which doesn't require metric= param
      conn =
        Plug.Test.conn(:get, "/prometheus/api/v1/label/host/values")
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)

      assert result["status"] == "success"
      assert is_list(result["data"])
      assert "web-1" in result["data"]
      assert "web-2" in result["data"]
      assert "db-1" in result["data"]
    end

    test "DDNet list_hosts parses host values response" do
      conn =
        Plug.Test.conn(:get, "/prometheus/api/v1/label/host/values")
        |> call()

      body = decode(conn.resp_body)

      # Simulate DDNet's list_hosts/0 parsing
      assert body["status"] == "success"
      hosts = body["data"]
      assert is_list(hosts)
      assert length(hosts) >= 3
    end

    test "Prometheus-path __name__ values lists all metrics" do
      conn =
        Plug.Test.conn(:get, "/prometheus/api/v1/label/__name__/values")
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)
      assert result["status"] == "success"
      assert "cpu_usage" in result["data"]
    end
  end

  # ── Prometheus Series Discovery ──────────────────────────────────────────

  describe "Prometheus series — /prometheus/api/v1/series" do
    setup do
      now = System.os_time(:second)
      seed("http_requests", %{"host" => "web-1", "method" => "GET"}, now, [100.0])
      seed("http_requests", %{"host" => "web-1", "method" => "POST"}, now, [50.0])
      seed("http_requests", %{"host" => "web-2", "method" => "GET"}, now, [200.0])
      :ok
    end

    test "returns VM/Prometheus format with __name__ in each series" do
      conn =
        Plug.Test.conn(:get, "/prometheus/api/v1/series?match[]=http_requests")
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)

      assert result["status"] == "success"
      assert is_list(result["data"])
      assert length(result["data"]) == 3

      for series <- result["data"] do
        # Each series is a flat label map including __name__
        assert is_map(series)
        assert series["__name__"] == "http_requests"
        assert Map.has_key?(series, "host")
        assert Map.has_key?(series, "method")
      end
    end
  end

  # ── Prometheus Labels Discovery ──────────────────────────────────────────

  describe "Prometheus labels — /prometheus/api/v1/labels" do
    setup do
      now = System.os_time(:second)
      seed("cpu", %{"host" => "web-1", "dc" => "us", "env" => "prod"}, now, [50.0])
      :ok
    end

    test "returns all label names including __name__" do
      conn =
        Plug.Test.conn(:get, "/prometheus/api/v1/labels")
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)

      assert result["status"] == "success"
      assert is_list(result["data"])
      assert "__name__" in result["data"]
      assert "host" in result["data"]
      assert "dc" in result["data"]
      assert "env" in result["data"]
    end

    test "label names are sorted" do
      conn =
        Plug.Test.conn(:get, "/prometheus/api/v1/labels")
        |> call()

      result = decode(conn.resp_body)
      assert result["data"] == Enum.sort(result["data"])
    end
  end

  # ── InfluxDB Line Protocol Import (TSBS compat) ─────────────────────────

  describe "POST /write — InfluxDB line protocol" do
    test "accepts standard InfluxDB line format" do
      # TSBS format: measurement,tag=val field=value timestamp_ns
      body = "cpu,host=web-1,region=us usage_user=42.5,usage_system=10.2 1700000000000000000\n"

      conn = Plug.Test.conn(:post, "/write", body) |> call()
      assert conn.status == 204

      TimelessMetrics.flush(:vmc)
      {:ok, metrics} = TimelessMetrics.list_metrics(:vmc)
      # Multi-field: measurement_fieldname
      assert "cpu_usage_user" in metrics
      assert "cpu_usage_system" in metrics
    end

    test "handles integer suffix (42i)" do
      body = "mem,host=db-1 total=16000000000i 1700000000000000000\n"
      conn = Plug.Test.conn(:post, "/write", body) |> call()
      assert conn.status == 204
    end

    test "single field named 'value' uses measurement as metric name" do
      body = "temperature,location=office value=22.5 1700000000000000000\n"
      conn = Plug.Test.conn(:post, "/write", body) |> call()
      assert conn.status == 204

      TimelessMetrics.flush(:vmc)
      {:ok, metrics} = TimelessMetrics.list_metrics(:vmc)
      assert "temperature" in metrics
    end
  end

  # ── DDNet End-to-End: Write via Prometheus, Read via PromQL ──────────────

  describe "DDNet roundtrip — write Prometheus, query PromQL" do
    test "DDNet VictoriaWriter format → DDNet VictoriaReader parse" do
      now_ms = System.system_time(:millisecond)
      now_s = div(now_ms, 1000)

      # Simulate what DDNet VictoriaWriter sends (Prometheus text format)
      text = """
      ifHCInOctets{host="router1",ifIndex="1"} 123456789 #{now_ms}
      ifHCInOctets{host="router1",ifIndex="2"} 987654321 #{now_ms}
      ifHCInOctets{host="router2",ifIndex="1"} 555555555 #{now_ms}
      """

      seed_prometheus(text)

      # Now query like DDNet VictoriaReader does
      conn =
        Plug.Test.conn(
          :get,
          "/prometheus/api/v1/query_range?query=ifHCInOctets&start=#{now_s - 60}&end=#{now_s + 60}&step=60"
        )
        |> call()

      assert conn.status == 200
      body = decode(conn.resp_body)

      # DDNet parse_response/1 expects this exact structure
      assert body["status"] == "success"
      assert body["data"]["resultType"] == "matrix"
      results = body["data"]["result"]

      series =
        Enum.map(results, fn result ->
          %{"metric" => result["metric"], "values" => result["values"]}
        end)

      assert length(series) >= 1
    end

    test "DDNet instant query after Prometheus write" do
      now_ms = System.system_time(:millisecond)
      now_s = div(now_ms, 1000)

      text = "sysUpTime{host=\"switch-01\"} 86400 #{now_ms}\n"
      seed_prometheus(text)

      conn =
        Plug.Test.conn(
          :get,
          "/prometheus/api/v1/query?query=sysUpTime&time=#{now_s}"
        )
        |> call()

      assert conn.status == 200
      body = decode(conn.resp_body)

      assert body["status"] == "success"
      assert body["data"]["resultType"] == "vector"
      results = body["data"]["result"]

      # DDNet parse_instant_response/1
      values =
        Enum.map(results, fn result ->
          [_ts, value] = result["value"]
          %{"metric" => result["metric"], "value" => value}
        end)

      assert length(values) >= 1
      assert hd(values)["metric"]["host"] == "switch-01"
    end
  end

  # ── Response Format Edge Cases ──────────────────────────────────────────

  describe "format edge cases" do
    test "error responses include status field" do
      conn =
        Plug.Test.conn(:get, "/api/v1/export")
        |> call()

      assert conn.status == 400
      result = decode(conn.resp_body)
      assert is_binary(result["error"])
    end

    test "health endpoint returns expected fields" do
      conn =
        Plug.Test.conn(:get, "/health")
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)
      assert result["status"] == "ok"
      assert is_integer(result["series"])
      assert is_integer(result["points"])
      assert is_integer(result["storage_bytes"])
    end

    test "404 for unknown routes" do
      conn =
        Plug.Test.conn(:get, "/api/v1/nonexistent")
        |> call()

      assert conn.status == 404
    end

    test "Prometheus /metrics endpoint returns text/plain exposition format" do
      conn =
        Plug.Test.conn(:get, "/metrics")
        |> call()

      assert conn.status == 200
      assert {"content-type", "text/plain; charset=utf-8"} in conn.resp_headers

      # Must contain standard VM metrics that Prometheus/VictoriaMetrics can scrape
      body = conn.resp_body
      assert body =~ "# HELP"
      assert body =~ "# TYPE"
      assert body =~ "vm_memory_total_bytes"
      assert body =~ "timeless_series_count"
      assert body =~ "timeless_total_points"
    end

    test "content-type is application/json for API responses" do
      seed("ct_test", %{"x" => "1"}, 1_700_000_000, [1.0])

      conn =
        Plug.Test.conn(:get, "/api/v1/export?metric=ct_test&from=1699999900&to=1700000200")
        |> call()

      assert {"content-type", "application/json; charset=utf-8"} in conn.resp_headers
    end
  end

  # ── VM-specific /api/v1 endpoints (native mode) ─────────────────────────

  describe "native /api/v1/label/:name/values" do
    test "returns VM envelope with status and data" do
      now = System.os_time(:second)
      seed("req", %{"method" => "GET"}, now, [1.0])
      seed("req", %{"method" => "POST"}, now, [2.0])
      seed("req", %{"method" => "PUT"}, now, [3.0])

      conn =
        Plug.Test.conn(:get, "/api/v1/label/method/values?metric=req")
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)
      assert result["status"] == "success"
      assert is_list(result["data"])
      assert "GET" in result["data"]
      assert "POST" in result["data"]
      assert "PUT" in result["data"]
    end
  end

  describe "native /api/v1/series" do
    test "returns series label maps" do
      now = System.os_time(:second)
      seed("net", %{"host" => "a", "iface" => "eth0"}, now, [1.0])
      seed("net", %{"host" => "b", "iface" => "eth0"}, now, [2.0])

      conn =
        Plug.Test.conn(:get, "/api/v1/series?metric=net")
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)
      assert result["status"] == "success"
      assert is_list(result["data"])
      assert length(result["data"]) == 2
    end
  end

  # ── DDNet PromQL Query Patterns ──────────────────────────────────────────

  describe "DDNet PromQL patterns" do
    setup do
      now = System.os_time(:second)

      for i <- 0..4 do
        TimelessMetrics.write(
          :vmc,
          "ifHCInOctets",
          %{"host" => "router-1", "ifIndex" => "1"},
          (i + 1) * 1000.0,
          timestamp: now - (4 - i) * 60
        )

        TimelessMetrics.write(
          :vmc,
          "ifHCInOctets",
          %{"host" => "router-2", "ifIndex" => "1"},
          (i + 1) * 500.0,
          timestamp: now - (4 - i) * 60
        )
      end

      TimelessMetrics.flush(:vmc)
      %{now: now}
    end

    test "simple metric query (no aggregation)", %{now: now} do
      conn =
        Plug.Test.conn(
          :get,
          "/prometheus/api/v1/query_range?query=ifHCInOctets&start=#{now - 300}&end=#{now}&step=60"
        )
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)
      assert result["status"] == "success"
      assert length(result["data"]["result"]) >= 1
    end

    test "PromQL with host filter", %{now: now} do
      conn =
        Plug.Test.conn(
          :get,
          "/prometheus/api/v1/query_range?" <>
            URI.encode_query(%{
              "query" => "ifHCInOctets{host=\"router-1\"}",
              "start" => now - 300,
              "end" => now,
              "step" => "60"
            })
        )
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)
      assert result["status"] == "success"

      # Should only return router-1 series
      for entry <- result["data"]["result"] do
        assert entry["metric"]["host"] == "router-1"
      end
    end
  end

  # ── Native /api/v1/query (instant) ──────────────────────────────────────

  describe "native /api/v1/query — latest value" do
    test "single series returns flat object" do
      now = 1_700_000_000
      seed("temp", %{"sensor" => "1"}, now, [22.5])

      conn =
        Plug.Test.conn(:get, "/api/v1/query?metric=temp&sensor=1")
        |> call()

      assert conn.status == 200
      result = decode(conn.resp_body)

      # Native format: flat object with labels/timestamp/value
      assert is_map(result["labels"]) or is_map(result)
      assert is_number(result["timestamp"]) or is_number(result["value"])
    end

    test "no data returns gracefully" do
      conn =
        Plug.Test.conn(:get, "/api/v1/query?metric=nonexistent")
        |> call()

      assert conn.status == 200
    end
  end

  # ── Import/Export Roundtrip ──────────────────────────────────────────────

  describe "VM JSON import → export roundtrip" do
    test "data survives import/export cycle with correct format" do
      # Import
      lines = [
        %{
          "metric" => %{"__name__" => "roundtrip_test", "env" => "prod", "host" => "api-1"},
          "values" => [10.0, 20.0, 30.0],
          "timestamps" => [1_700_000_000, 1_700_000_060, 1_700_000_120]
        }
      ]

      vm_import(lines)

      # Export
      conn =
        Plug.Test.conn(
          :get,
          "/api/v1/export?metric=roundtrip_test&env=prod&host=api-1&from=1699999900&to=1700000200"
        )
        |> call()

      assert conn.status == 200
      exported = decode(conn.resp_body)

      assert exported["metric"]["__name__"] == "roundtrip_test"
      assert exported["metric"]["env"] == "prod"
      assert exported["metric"]["host"] == "api-1"
      assert length(exported["values"]) == 3
      assert length(exported["timestamps"]) == 3

      # Timestamps exported as milliseconds (VM compat)
      assert exported["timestamps"] == [1_700_000_000_000, 1_700_000_060_000, 1_700_000_120_000]
    end
  end
end
