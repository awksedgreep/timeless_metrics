defmodule TimelessMetrics.PrometheusTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_prom_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {TimelessMetrics,
       name: :prom_test, data_dir: @data_dir, buffer_shards: 1, segment_duration: 3_600}
    )

    on_exit(fn ->
      :persistent_term.erase({TimelessMetrics, :prom_test, :schema})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  # --- Prometheus text format import ---

  test "import prometheus text format with labels and millisecond timestamps" do
    now_ms = System.os_time(:millisecond)
    now_s = div(now_ms, 1000)

    body = """
    cpu_usage{host="web-1",region="us-east"} 73.2 #{now_ms}
    cpu_usage{host="web-2",region="us-west"} 81.0 #{now_ms}
    """

    conn =
      Plug.Test.conn(:post, "/api/v1/import/prometheus", body)
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> TimelessMetrics.HTTP.call(store: :prom_test)

    assert conn.status == 204

    TimelessMetrics.flush(:prom_test)

    {:ok, results} =
      TimelessMetrics.query_multi(:prom_test, "cpu_usage", %{}, from: now_s - 60, to: now_s + 60)

    assert length(results) == 2

    web1 = Enum.find(results, fn %{labels: l} -> l["host"] == "web-1" end)
    assert web1 != nil
    [{_ts, val}] = web1.points
    assert_in_delta val, 73.2, 0.01
  end

  test "import prometheus text format without labels" do
    now_ms = System.os_time(:millisecond)
    now_s = div(now_ms, 1000)

    body = "up 1.0 #{now_ms}\n"

    conn =
      Plug.Test.conn(:post, "/api/v1/import/prometheus", body)
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> TimelessMetrics.HTTP.call(store: :prom_test)

    assert conn.status == 204

    TimelessMetrics.flush(:prom_test)

    {:ok, results} =
      TimelessMetrics.query_multi(:prom_test, "up", %{}, from: now_s - 60, to: now_s + 60)

    assert length(results) == 1
    [{_ts, val}] = List.first(results).points
    assert_in_delta val, 1.0, 0.01
  end

  test "import skips comment and HELP/TYPE lines" do
    now_ms = System.os_time(:millisecond)
    now_s = div(now_ms, 1000)

    body = """
    # HELP cpu_usage CPU utilization percentage
    # TYPE cpu_usage gauge
    cpu_usage{host="web-1"} 55.5 #{now_ms}
    # This is a comment
    """

    conn =
      Plug.Test.conn(:post, "/api/v1/import/prometheus", body)
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> TimelessMetrics.HTTP.call(store: :prom_test)

    assert conn.status == 204

    TimelessMetrics.flush(:prom_test)

    {:ok, results} =
      TimelessMetrics.query_multi(:prom_test, "cpu_usage", %{}, from: now_s - 60, to: now_s + 60)

    assert length(results) == 1
  end

  test "import without timestamp uses current time" do
    body = "my_gauge{env=\"prod\"} 42.0\n"

    conn =
      Plug.Test.conn(:post, "/api/v1/import/prometheus", body)
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> TimelessMetrics.HTTP.call(store: :prom_test)

    assert conn.status == 204

    TimelessMetrics.flush(:prom_test)

    now = System.os_time(:second)

    {:ok, results} =
      TimelessMetrics.query_multi(:prom_test, "my_gauge", %{}, from: now - 10, to: now + 10)

    assert length(results) == 1
  end

  test "import reports errors for malformed lines" do
    body = """
    valid_metric{host="a"} 10.0
    this is not valid prometheus format
    another_valid{host="b"} 20.0
    """

    conn =
      Plug.Test.conn(:post, "/api/v1/import/prometheus", body)
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> TimelessMetrics.HTTP.call(store: :prom_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert result["samples"] == 2
    assert result["errors"] == 1
  end

  # --- Prometheus-compatible query_range endpoint ---

  test "prometheus query_range returns matrix format" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..9 do
      TimelessMetrics.write(:prom_test, "cpu_usage", %{"host" => "web-1"}, 50.0 + i,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:prom_test)

    conn =
      Plug.Test.conn(
        :get,
        "/prometheus/api/v1/query_range?query=cpu_usage&start=#{base}&end=#{base + 600}&step=60"
      )
      |> TimelessMetrics.HTTP.call(store: :prom_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert result["status"] == "success"
    assert result["data"]["resultType"] == "matrix"

    series = result["data"]["result"]
    assert length(series) >= 1

    first = List.first(series)
    assert first["metric"]["__name__"] == "cpu_usage"
    assert first["metric"]["host"] == "web-1"
    assert length(first["values"]) > 0

    # Each value is [timestamp, string_value]
    [ts, val_str] = List.first(first["values"])
    assert is_integer(ts)
    assert is_binary(val_str)
    {val, _} = Float.parse(val_str)
    assert val >= 50.0
  end

  test "prometheus query_range with label filter" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:prom_test, "cpu_usage", %{"host" => "web-1"}, 50.0,
        timestamp: base + i * 60
      )

      TimelessMetrics.write(:prom_test, "cpu_usage", %{"host" => "web-2"}, 70.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:prom_test)

    # Query with PromQL-style label filter
    conn =
      Plug.Test.conn(
        :get,
        ~s(/prometheus/api/v1/query_range?query=cpu_usage%7Bhost%3D%22web-1%22%7D&start=#{base}&end=#{base + 600}&step=60)
      )
      |> TimelessMetrics.HTTP.call(store: :prom_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    series = result["data"]["result"]
    assert length(series) == 1
    assert List.first(series)["metric"]["host"] == "web-1"
  end

  test "prometheus query_range with duration step" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:prom_test, "cpu_usage", %{"host" => "web-1"}, 50.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:prom_test)

    conn =
      Plug.Test.conn(
        :get,
        "/prometheus/api/v1/query_range?query=cpu_usage&start=#{base}&end=#{base + 600}&step=5m"
      )
      |> TimelessMetrics.HTTP.call(store: :prom_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert result["status"] == "success"
    # With 5-minute step over a 10-minute range, we should get 1-2 buckets
    series = result["data"]["result"]
    assert length(series) >= 1
  end

  test "prometheus query_range returns error for missing query" do
    conn =
      Plug.Test.conn(:get, "/prometheus/api/v1/query_range?start=1000&end=2000&step=60")
      |> TimelessMetrics.HTTP.call(store: :prom_test)

    assert conn.status == 400
    result = Jason.decode!(conn.resp_body)
    assert result["error"] =~ "query"
  end
end
