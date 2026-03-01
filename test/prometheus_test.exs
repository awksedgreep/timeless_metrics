defmodule TimelessMetrics.PrometheusTest do
  use ExUnit.Case, async: false

  setup do
    id = System.unique_integer([:positive])
    store = :"prom_test_#{id}"
    data_dir = "/tmp/timeless_prom_test_#{id}"

    start_supervised!({TimelessMetrics, name: store, data_dir: data_dir, engine: :actor})

    on_exit(fn -> File.rm_rf!(data_dir) end)

    {:ok, store: store}
  end

  # --- Prometheus text format import ---

  test "import prometheus text format with labels and millisecond timestamps", %{store: store} do
    now_ms = System.os_time(:millisecond)
    now_s = div(now_ms, 1000)

    body = """
    cpu_usage{host="web-1",region="us-east"} 73.2 #{now_ms}
    cpu_usage{host="web-2",region="us-west"} 81.0 #{now_ms}
    """

    conn =
      Plug.Test.conn(:post, "/api/v1/import/prometheus", body)
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 204

    TimelessMetrics.flush(store)

    {:ok, results} =
      TimelessMetrics.query_multi(store, "cpu_usage", %{}, from: now_s - 60, to: now_s + 60)

    assert length(results) == 2

    web1 = Enum.find(results, fn %{labels: l} -> l["host"] == "web-1" end)
    assert web1 != nil
    [{_ts, val}] = web1.points
    assert_in_delta val, 73.2, 0.01
  end

  test "import prometheus text format without labels", %{store: store} do
    now_ms = System.os_time(:millisecond)
    now_s = div(now_ms, 1000)

    body = "up 1.0 #{now_ms}\n"

    conn =
      Plug.Test.conn(:post, "/api/v1/import/prometheus", body)
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 204

    TimelessMetrics.flush(store)

    {:ok, results} =
      TimelessMetrics.query_multi(store, "up", %{}, from: now_s - 60, to: now_s + 60)

    assert length(results) == 1
    [{_ts, val}] = List.first(results).points
    assert_in_delta val, 1.0, 0.01
  end

  test "import skips comment and HELP/TYPE lines", %{store: store} do
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
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 204

    TimelessMetrics.flush(store)

    {:ok, results} =
      TimelessMetrics.query_multi(store, "cpu_usage", %{}, from: now_s - 60, to: now_s + 60)

    assert length(results) == 1
  end

  test "import without timestamp uses current time", %{store: store} do
    body = "my_gauge{env=\"prod\"} 42.0\n"

    conn =
      Plug.Test.conn(:post, "/api/v1/import/prometheus", body)
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 204

    TimelessMetrics.flush(store)

    now = System.os_time(:second)

    {:ok, results} =
      TimelessMetrics.query_multi(store, "my_gauge", %{}, from: now - 10, to: now + 10)

    assert length(results) == 1
  end

  test "import reports errors for malformed lines", %{store: store} do
    body = """
    valid_metric{host="a"} 10.0
    this is not valid prometheus format
    another_valid{host="b"} 20.0
    """

    conn =
      Plug.Test.conn(:post, "/api/v1/import/prometheus", body)
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    assert result["samples"] == 2
    assert result["errors"] == 1
  end

  # --- Prometheus-compatible query_range endpoint ---

  test "prometheus query_range returns matrix format", %{store: store} do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..9 do
      TimelessMetrics.write(store, "cpu_usage", %{"host" => "web-1"}, 50.0 + i,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(store)

    conn =
      Plug.Test.conn(
        :get,
        "/prometheus/api/v1/query_range?query=cpu_usage&start=#{base}&end=#{base + 600}&step=60"
      )
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
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

  test "prometheus query_range with label filter", %{store: store} do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(store, "cpu_usage", %{"host" => "web-1"}, 50.0,
        timestamp: base + i * 60
      )

      TimelessMetrics.write(store, "cpu_usage", %{"host" => "web-2"}, 70.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(store)

    # Query with PromQL-style label filter
    conn =
      Plug.Test.conn(
        :get,
        ~s(/prometheus/api/v1/query_range?query=cpu_usage%7Bhost%3D%22web-1%22%7D&start=#{base}&end=#{base + 600}&step=60)
      )
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    series = result["data"]["result"]
    assert length(series) == 1
    assert List.first(series)["metric"]["host"] == "web-1"
  end

  test "prometheus query_range with duration step", %{store: store} do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(store, "cpu_usage", %{"host" => "web-1"}, 50.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(store)

    conn =
      Plug.Test.conn(
        :get,
        "/prometheus/api/v1/query_range?query=cpu_usage&start=#{base}&end=#{base + 600}&step=5m"
      )
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    assert result["status"] == "success"
    # With 5-minute step over a 10-minute range, we should get 1-2 buckets
    series = result["data"]["result"]
    assert length(series) >= 1
  end

  test "prometheus query_range returns error for missing query", %{store: store} do
    conn =
      Plug.Test.conn(:get, "/prometheus/api/v1/query_range?start=1000&end=2000&step=60")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 400
    result = :json.decode(conn.resp_body)
    assert result["error"] =~ "query"
  end

  # --- Prometheus-compatible instant query endpoint ---

  test "prometheus query returns vector format with last value", %{store: store} do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..4 do
      TimelessMetrics.write(store, "mem_usage", %{"host" => "web-1"}, 100.0 + i,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(store)

    conn =
      Plug.Test.conn(:get, "/prometheus/api/v1/query?query=mem_usage&time=#{base + 300}")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    assert result["status"] == "success"
    assert result["data"]["resultType"] == "vector"

    series = result["data"]["result"]
    assert length(series) >= 1

    first = List.first(series)
    assert first["metric"]["__name__"] == "mem_usage"
    # value is [timestamp, string_value]
    [ts, val_str] = first["value"]
    assert is_integer(ts)
    assert is_binary(val_str)
  end

  test "prometheus query returns error for missing query param", %{store: store} do
    conn =
      Plug.Test.conn(:get, "/prometheus/api/v1/query")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 400
    result = :json.decode(conn.resp_body)
    assert result["error"] =~ "query"
  end

  test "prometheus query with label filter", %{store: store} do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    TimelessMetrics.write(store, "cpu", %{"host" => "a"}, 10.0, timestamp: base)
    TimelessMetrics.write(store, "cpu", %{"host" => "b"}, 20.0, timestamp: base)
    TimelessMetrics.flush(store)

    conn =
      Plug.Test.conn(
        :get,
        ~s(/prometheus/api/v1/query?query=cpu%7Bhost%3D%22a%22%7D&time=#{base + 300})
      )
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    series = result["data"]["result"]
    assert length(series) == 1
    assert List.first(series)["metric"]["host"] == "a"
  end

  # --- Prometheus-compatible labels endpoint ---

  test "prometheus labels returns all label names including __name__", %{store: store} do
    now = System.os_time(:second)

    TimelessMetrics.write(store, "cpu_usage", %{"host" => "web-1", "region" => "us"}, 50.0,
      timestamp: now
    )

    TimelessMetrics.write(store, "mem_usage", %{"host" => "web-1", "env" => "prod"}, 1024.0,
      timestamp: now
    )

    TimelessMetrics.flush(store)

    conn =
      Plug.Test.conn(:get, "/prometheus/api/v1/labels")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    assert result["status"] == "success"

    labels = result["data"]
    assert "__name__" in labels
    assert "host" in labels
    assert "region" in labels
    assert "env" in labels
    # Should be sorted
    assert labels == Enum.sort(labels)
  end

  test "prometheus labels returns only __name__ when no labels exist", %{store: store} do
    now = System.os_time(:second)
    TimelessMetrics.write(store, "simple_metric", %{}, 42.0, timestamp: now)
    TimelessMetrics.flush(store)

    conn =
      Plug.Test.conn(:get, "/prometheus/api/v1/labels")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    assert "__name__" in result["data"]
  end

  # --- Prometheus-compatible label values endpoint ---

  test "prometheus label __name__ values returns all metric names", %{store: store} do
    now = System.os_time(:second)
    TimelessMetrics.write(store, "cpu_usage", %{"host" => "a"}, 1.0, timestamp: now)
    TimelessMetrics.write(store, "mem_usage", %{"host" => "a"}, 2.0, timestamp: now)
    TimelessMetrics.flush(store)

    conn =
      Plug.Test.conn(:get, "/prometheus/api/v1/label/__name__/values")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    assert result["status"] == "success"
    assert "cpu_usage" in result["data"]
    assert "mem_usage" in result["data"]
  end

  test "prometheus label values returns deduplicated values across metrics", %{store: store} do
    now = System.os_time(:second)
    TimelessMetrics.write(store, "cpu", %{"host" => "web-1"}, 1.0, timestamp: now)
    TimelessMetrics.write(store, "cpu", %{"host" => "web-2"}, 2.0, timestamp: now)
    TimelessMetrics.write(store, "mem", %{"host" => "web-1"}, 3.0, timestamp: now)
    TimelessMetrics.flush(store)

    conn =
      Plug.Test.conn(:get, "/prometheus/api/v1/label/host/values")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    values = result["data"]
    assert "web-1" in values
    assert "web-2" in values
    # Deduplicated — web-1 appears in both cpu and mem but only once in result
    assert length(Enum.filter(values, &(&1 == "web-1"))) == 1
    # Should be sorted
    assert values == Enum.sort(values)
  end

  test "prometheus label values for unknown label returns empty list", %{store: store} do
    now = System.os_time(:second)
    TimelessMetrics.write(store, "cpu", %{"host" => "a"}, 1.0, timestamp: now)
    TimelessMetrics.flush(store)

    conn =
      Plug.Test.conn(:get, "/prometheus/api/v1/label/nonexistent/values")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    assert result["data"] == []
  end

  # --- Prometheus-compatible series endpoint ---

  test "prometheus series returns matching series with __name__", %{store: store} do
    now = System.os_time(:second)

    TimelessMetrics.write(store, "cpu", %{"host" => "web-1", "region" => "us"}, 1.0,
      timestamp: now
    )

    TimelessMetrics.write(store, "cpu", %{"host" => "web-2", "region" => "eu"}, 2.0,
      timestamp: now
    )

    TimelessMetrics.flush(store)

    conn =
      Plug.Test.conn(:get, "/prometheus/api/v1/series?match[]=cpu")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    assert result["status"] == "success"

    series = result["data"]
    assert length(series) == 2
    assert Enum.all?(series, &(&1["__name__"] == "cpu"))
    hosts = Enum.map(series, & &1["host"]) |> Enum.sort()
    assert hosts == ["web-1", "web-2"]
  end

  test "prometheus series with label filter in match[]", %{store: store} do
    now = System.os_time(:second)
    TimelessMetrics.write(store, "cpu", %{"host" => "web-1"}, 1.0, timestamp: now)
    TimelessMetrics.write(store, "cpu", %{"host" => "web-2"}, 2.0, timestamp: now)
    TimelessMetrics.flush(store)

    conn =
      Plug.Test.conn(
        :get,
        ~s(/prometheus/api/v1/series?match[]=cpu%7Bhost%3D%22web-1%22%7D)
      )
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    series = result["data"]
    assert length(series) == 1
    assert List.first(series)["host"] == "web-1"
  end

  test "prometheus series returns error for missing match[]", %{store: store} do
    conn =
      Plug.Test.conn(:get, "/prometheus/api/v1/series")
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 400
    result = :json.decode(conn.resp_body)
    assert result["error"] =~ "match"
  end

  test "prometheus series with regex match", %{store: store} do
    now = System.os_time(:second)
    TimelessMetrics.write(store, "cpu_user", %{"host" => "a"}, 1.0, timestamp: now)
    TimelessMetrics.write(store, "cpu_system", %{"host" => "a"}, 2.0, timestamp: now)
    TimelessMetrics.write(store, "mem_usage", %{"host" => "a"}, 3.0, timestamp: now)
    TimelessMetrics.flush(store)

    conn =
      Plug.Test.conn(
        :get,
        ~s(/prometheus/api/v1/series?match[]=%7B__name__%3D~%22cpu_.*%22%7D)
      )
      |> TimelessMetrics.HTTP.call(store: store)

    assert conn.status == 200
    result = :json.decode(conn.resp_body)
    series = result["data"]
    assert length(series) == 2
    names = Enum.map(series, & &1["__name__"]) |> Enum.sort()
    assert names == ["cpu_system", "cpu_user"]
  end
end
