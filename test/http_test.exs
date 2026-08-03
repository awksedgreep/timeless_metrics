defmodule TimelessMetrics.HTTPTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper

  @data_dir "/tmp/timeless_http_test_#{System.os_time(:millisecond)}"
  @port 18_410

  setup_all do
    start_supervised!({TimelessMetrics.HTTP, store: :http_test, port: @port})
    Process.sleep(50)

    on_exit(fn ->
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, nil})
    end)

    :ok
  end

  setup do
    TestHelper.await_down(:http_test_sup)
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, nil})
    start_supervised!({TimelessMetrics, name: :http_test, data_dir: @data_dir})

    on_exit(fn ->
      TestHelper.await_down(:http_test_sup)
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, nil})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "POST /api/v1/import ingests JSON lines" do
    now_s = 1_700_000_000
    now_ms = now_s * 1000

    lines =
      Enum.join(
        [
          :json.encode(%{
            metric: %{__name__: "cpu_usage", host: "web-1"},
            values: [73.2, 74.1],
            timestamps: [now_ms, now_ms + 60_000]
          })
          |> IO.iodata_to_binary(),
          :json.encode(%{
            metric: %{__name__: "mem_usage", host: "web-1"},
            values: [45.0],
            timestamps: [now_ms]
          })
          |> IO.iodata_to_binary()
        ],
        "\n"
      )

    resp = TimelessMetrics.TestHTTP.post(@port, "/api/v1/import", lines)

    assert resp.status == 204

    flush = TimelessMetrics.TestHTTP.post(@port, "/api/v1/flush", "")
    assert flush.status == 200

    assert %{
             "status" => "ok",
             "admitted_batches" => 1,
             "completed_batches" => 1,
             "completed_points" => 3
           } = :json.decode(flush.body)

    {:ok, cpu_points} =
      TimelessMetrics.query(:http_test, "cpu_usage", %{"host" => "web-1"},
        from: now_s - 60,
        to: now_s + 120
      )

    assert length(cpu_points) == 2
    assert {^now_s, v1} = List.first(cpu_points)
    assert_in_delta v1, 73.2, 0.01

    {:ok, mem_points} =
      TimelessMetrics.query(:http_test, "mem_usage", %{"host" => "web-1"},
        from: now_s - 60,
        to: now_s + 60
      )

    assert [{^now_s, 45.0}] = mem_points
  end

  test "POST /api/v1/import handles multiple series in batch" do
    now = 1_700_000_000

    lines =
      for i <- 1..50 do
        :json.encode(%{
          metric: %{__name__: "cpu", host: "host-#{i}"},
          values: [i * 1.0],
          timestamps: [now]
        })
        |> IO.iodata_to_binary()
      end
      |> Enum.join("\n")

    resp = TimelessMetrics.TestHTTP.post(@port, "/api/v1/import", lines)

    assert resp.status == 204

    TimelessMetrics.flush(:http_test)

    info = TimelessMetrics.info(:http_test)
    assert info.series_count == 50
  end

  test "GET /health returns store stats" do
    resp = TimelessMetrics.TestHTTP.get(@port, "/health")

    assert resp.status == 200
    body = :json.decode(resp.body)
    assert body["status"] == "ok"
    assert is_integer(body["series"])
    assert is_integer(body["points"])
    assert is_integer(body["completed_points"])
    assert is_integer(body["admitted_batches"])
    assert is_integer(body["completed_batches"])
    assert is_integer(body["queued_batches"])
    assert is_integer(body["in_flight_batches"])
    assert is_integer(body["oldest_queued_ms"])
    assert is_integer(body["import_errors"])
    assert is_integer(body["queries"])
  end

  test "GET /health returns store stats for rust engine stores" do
    rust_dir = "/tmp/timeless_http_rust_test_#{System.unique_integer([:positive])}"
    rust_port = @port + 1

    start_supervised!({TimelessMetrics, name: :http_rust_test, data_dir: rust_dir, engine: :rust})
    start_supervised!({TimelessMetrics.HTTP, store: :http_rust_test, port: rust_port})
    Process.sleep(50)

    on_exit(fn ->
      File.rm_rf!(rust_dir)
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, nil})
    end)

    resp = TimelessMetrics.TestHTTP.get(rust_port, "/health")

    assert resp.status == 200
    body = :json.decode(resp.body)
    assert body["status"] == "ok"
    assert is_integer(body["series"])
    assert is_integer(body["points"])
    assert is_integer(body["buffer_points"])
    assert is_integer(body["queries"])
  end

  test "unknown route returns 404" do
    resp = TimelessMetrics.TestHTTP.get(@port, "/nonexistent")

    assert resp.status == 404
  end

  # --- Query endpoints ---

  test "GET /api/v1/export returns raw points in VM format" do
    now = 1_700_000_000
    seed_points(:http_test, "cpu_usage", %{"host" => "web-1"}, now, [73.2, 74.1, 75.0])

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/export?metric=cpu_usage&host=web-1&from=#{now - 60}&to=#{now + 120}"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["metric"]["__name__"] == "cpu_usage"
    assert result["metric"]["host"] == "web-1"
    assert length(result["values"]) == 3
    assert length(result["timestamps"]) == 3
  end

  test "GET /api/v1/export returns empty body for no data" do
    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/export?metric=nonexistent&from=0&to=9999999999"
      )

    assert resp.status == 200
    assert resp.body == ""
  end

  test "GET /api/v1/export requires metric param" do
    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/export?from=0&to=9999999999")

    assert resp.status == 400
    assert :json.decode(resp.body)["error"] =~ "metric"
  end

  test "GET /api/v1/query returns latest value" do
    now = 1_700_000_000
    seed_points(:http_test, "mem_usage", %{"host" => "db-1"}, now, [40.0, 45.0, 50.0])

    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/query?metric=mem_usage&host=db-1")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["timestamp"] == now + 2
    assert_in_delta result["value"], 50.0, 0.01
  end

  test "GET /api/v1/query returns null for missing series" do
    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/query?metric=nonexistent")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["timestamp"] == nil
    assert result["value"] == nil
  end

  test "GET /api/v1/query_range returns bucketed aggregation" do
    # Use current time so points fall within raw retention window
    now = System.os_time(:second)

    # Write 10 points, 1 per second
    values = for i <- 0..9, do: i * 10.0
    seed_points(:http_test, "req_rate", %{"svc" => "api"}, now, values)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/query_range?metric=req_rate&svc=api&from=#{now - 1}&to=#{now + 10}&step=5&aggregate=avg"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["metric"] == "req_rate"
    assert length(result["series"]) >= 1
    series = List.first(result["series"])
    assert series["labels"] == %{"svc" => "api"}
    assert length(series["data"]) >= 2
  end

  test "GET /api/v1/query_range supports different aggregates" do
    now = System.os_time(:second)
    seed_points(:http_test, "latency", %{"ep" => "/health"}, now, [10.0, 20.0, 30.0, 40.0, 50.0])

    for {agg, _expected} <- [{"min", 10.0}, {"max", 50.0}, {"sum", 150.0}, {"count", 5}] do
      resp =
        TimelessMetrics.TestHTTP.get(
          @port,
          "/api/v1/query_range?metric=latency&ep=/health&from=#{now - 1}&to=#{now + 10}&step=3600&aggregate=#{agg}"
        )

      assert resp.status == 200
      result = :json.decode(resp.body)
      series = result["series"]
      assert length(series) >= 1, "aggregate #{agg} returned no series"
      assert length(List.first(series)["data"]) >= 1, "aggregate #{agg} returned no data"
    end
  end

  test "GET /api/v1/query_range requires metric param" do
    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/query_range?from=0&to=9999999999&step=60")

    assert resp.status == 400
  end

  # --- Multi-series label filtering ---

  test "GET /api/v1/export with partial labels returns multiple series" do
    now = 1_700_000_000
    seed_points(:http_test, "cpu", %{"host" => "web-1", "dc" => "us"}, now, [10.0])
    seed_points(:http_test, "cpu", %{"host" => "web-2", "dc" => "us"}, now, [20.0])
    seed_points(:http_test, "cpu", %{"host" => "db-1", "dc" => "eu"}, now, [30.0])

    # Filter by dc=us only — should match 2 series
    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/export?metric=cpu&dc=us&from=#{now - 60}&to=#{now + 60}"
      )

    assert resp.status == 200
    lines = String.split(resp.body, "\n", trim: true)
    assert length(lines) == 2
  end

  test "GET /api/v1/export with no labels returns all series" do
    now = 1_700_000_000
    seed_points(:http_test, "mem", %{"host" => "a"}, now, [1.0])
    seed_points(:http_test, "mem", %{"host" => "b"}, now, [2.0])
    seed_points(:http_test, "mem", %{"host" => "c"}, now, [3.0])

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/export?metric=mem&from=#{now - 60}&to=#{now + 60}"
      )

    assert resp.status == 200
    lines = String.split(resp.body, "\n", trim: true)
    assert length(lines) == 3
  end

  test "GET /api/v1/query_range with partial labels returns multiple series" do
    now = System.os_time(:second)
    seed_points(:http_test, "req", %{"svc" => "api", "method" => "GET"}, now, [1.0, 2.0])
    seed_points(:http_test, "req", %{"svc" => "api", "method" => "POST"}, now, [3.0, 4.0])
    seed_points(:http_test, "req", %{"svc" => "web", "method" => "GET"}, now, [5.0, 6.0])

    # Filter by svc=api — should match 2 series
    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/query_range?metric=req&svc=api&from=#{now - 1}&to=#{now + 10}&step=3600&aggregate=avg"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert length(result["series"]) == 2
  end

  test "Elixir API query_multi with label filter" do
    now = System.os_time(:second)
    seed_points(:http_test, "disk", %{"host" => "a", "mount" => "/"}, now, [50.0])
    seed_points(:http_test, "disk", %{"host" => "a", "mount" => "/data"}, now, [80.0])
    seed_points(:http_test, "disk", %{"host" => "b", "mount" => "/"}, now, [30.0])

    # Filter host=a — 2 series
    {:ok, results} =
      TimelessMetrics.query_multi(:http_test, "disk", %{"host" => "a"},
        from: now - 60,
        to: now + 60
      )

    assert length(results) == 2
    assert Enum.all?(results, fn %{labels: l} -> l["host"] == "a" end)

    # No filter — all 3 series
    {:ok, all_results} =
      TimelessMetrics.query_multi(:http_test, "disk", %{},
        from: now - 60,
        to: now + 60
      )

    assert length(all_results) == 3
  end

  # --- Chart endpoint ---

  test "GET /chart returns SVG image" do
    now = System.os_time(:second)

    seed_points(:http_test, "chart_test", %{"host" => "web-1"}, now, [
      10.0,
      20.0,
      30.0,
      25.0,
      15.0
    ])

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/chart?metric=chart_test&host=web-1&from=#{now - 1}&to=#{now + 10}&step=1"
      )

    assert resp.status == 200
    assert {"content-type", "image/svg+xml"} in resp.headers
    assert resp.body =~ "<svg"
    assert resp.body =~ "chart_test"
    assert resp.body =~ "<polyline"
  end

  test "GET /chart with multiple series renders multi-line chart" do
    now = System.os_time(:second)
    seed_points(:http_test, "multi_chart", %{"host" => "a"}, now, [10.0, 20.0, 30.0])
    seed_points(:http_test, "multi_chart", %{"host" => "b"}, now, [30.0, 20.0, 10.0])

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/chart?metric=multi_chart&from=#{now - 1}&to=#{now + 10}&step=1"
      )

    assert resp.status == 200
    assert resp.body =~ "<svg"
    # Should have 2 polylines (one per series)
    assert length(Regex.scan(~r/<polyline/, resp.body)) == 2
  end

  test "GET /chart with custom dimensions" do
    now = System.os_time(:second)
    seed_points(:http_test, "sized_chart", %{"id" => "1"}, now, [1.0, 2.0, 3.0])

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/chart?metric=sized_chart&id=1&from=#{now - 1}&to=#{now + 10}&width=400&height=200&step=1"
      )

    assert resp.status == 200
    assert resp.body =~ ~s(width="400")
    assert resp.body =~ ~s(height="200")
  end

  test "GET /chart with no data renders empty state" do
    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/chart?metric=nonexistent&from=0&to=9999999999&step=60"
      )

    assert resp.status == 200
    assert resp.body =~ "No data"
  end

  test "GET /chart supports relative time" do
    now = System.os_time(:second)
    seed_points(:http_test, "rel_chart", %{"id" => "1"}, now - 1800, [1.0, 2.0, 3.0])

    resp =
      TimelessMetrics.TestHTTP.get(@port, "/chart?metric=rel_chart&id=1&from=-1h&to=now")

    assert resp.status == 200
    assert resp.body =~ "<polyline" or resp.body =~ "<circle"
  end

  test "GET /chart preserves the requested time domain for sparse data" do
    now = System.os_time(:second)
    start_ts = now - 86_400
    sparse_ts = start_ts + 1_200

    seed_points(:http_test, "sparse_chart", %{"host" => "web-1"}, sparse_ts, [10.0, 20.0])

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/chart?metric=sparse_chart&host=web-1&from=#{start_ts}&to=#{now}&step=300"
      )

    assert resp.status == 200

    xs = svg_x_positions(resp.body)

    assert Enum.max(xs) < 80.0
  end

  test "GET /chart requires metric param" do
    resp = TimelessMetrics.TestHTTP.get(@port, "/chart?from=-1h")

    assert resp.status == 400
  end

  defp svg_x_positions(svg) do
    polyline_xs =
      Regex.scan(~r/<polyline points="([^"]+)"/, svg, capture: :all_but_first)
      |> Enum.flat_map(fn [points] ->
        points
        |> String.split(" ", trim: true)
        |> Enum.map(fn point ->
          [x, _y] = String.split(point, ",")
          String.to_float(x)
        end)
      end)

    circle_xs =
      Regex.scan(~r/<circle cx="([^"]+)"/, svg, capture: :all_but_first)
      |> Enum.map(fn [x] -> String.to_float(x) end)

    polyline_xs ++ circle_xs
  end

  # --- Discovery endpoints ---

  test "GET /api/v1/label/__name__/values lists metric names" do
    now = System.os_time(:second)
    seed_points(:http_test, "cpu_usage", %{"host" => "a"}, now, [1.0])
    seed_points(:http_test, "mem_usage", %{"host" => "a"}, now, [2.0])
    seed_points(:http_test, "disk_io", %{"host" => "a"}, now, [3.0])

    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/label/__name__/values")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["status"] == "success"
    assert "cpu_usage" in result["data"]
    assert "mem_usage" in result["data"]
    assert "disk_io" in result["data"]
  end

  test "GET /api/v1/label/:name/values lists label values" do
    now = System.os_time(:second)
    seed_points(:http_test, "cpu", %{"host" => "web-1"}, now, [1.0])
    seed_points(:http_test, "cpu", %{"host" => "web-2"}, now, [2.0])
    seed_points(:http_test, "cpu", %{"host" => "db-1"}, now, [3.0])

    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/label/host/values?metric=cpu")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert length(result["data"]) == 3
    assert "web-1" in result["data"]
    assert "web-2" in result["data"]
    assert "db-1" in result["data"]
  end

  test "GET /api/v1/series lists series for a metric" do
    now = System.os_time(:second)
    seed_points(:http_test, "net_rx", %{"host" => "a", "iface" => "eth0"}, now, [1.0])
    seed_points(:http_test, "net_rx", %{"host" => "a", "iface" => "eth1"}, now, [2.0])
    seed_points(:http_test, "net_rx", %{"host" => "b", "iface" => "eth0"}, now, [3.0])

    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/series?metric=net_rx")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert length(result["data"]) == 3
  end

  test "label values without metric param returns all values (VM compat)" do
    now = System.os_time(:second)
    seed_points(:http_test, "cpu", %{"host" => "web-1"}, now, [1.0])
    seed_points(:http_test, "cpu", %{"host" => "web-2"}, now, [2.0])

    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/label/host/values")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert "web-1" in result["data"]
    assert "web-2" in result["data"]
  end

  test "series endpoint requires metric param" do
    resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/series")

    assert resp.status == 400
  end

  # --- Helpers ---

  defp seed_points(store, metric, labels, base_ts, values) do
    values
    |> Enum.with_index()
    |> Enum.each(fn {val, i} ->
      TimelessMetrics.write(store, metric, labels, val, timestamp: base_ts + i)
    end)

    TimelessMetrics.flush(store)
  end

  test "labels without __name__ default to 'unknown'" do
    body =
      :json.encode(%{
        metric: %{host: "web-1"},
        values: [99.0],
        timestamps: [1_700_000_000_000]
      })
      |> IO.iodata_to_binary()

    resp = TimelessMetrics.TestHTTP.post(@port, "/api/v1/import", body)

    assert resp.status == 204

    TimelessMetrics.flush(:http_test)

    {:ok, points} =
      TimelessMetrics.query(:http_test, "unknown", %{"host" => "web-1"},
        from: 1_699_999_900,
        to: 1_700_000_100
      )

    assert length(points) == 1
  end

  # --- Bearer Token Auth ---

  @secret "test-secret-token"

  test "auth disabled: requests work without token" do
    resp = TimelessMetrics.TestHTTP.get(@port, "/health")

    assert resp.status == 200
  end

  test "auth enabled: valid Bearer header returns 200" do
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, @secret})

    resp =
      TimelessMetrics.TestHTTP.get(@port, "/health",
        headers: [{"authorization", "Bearer #{@secret}"}]
      )

    assert resp.status == 200

    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, nil})
  end

  test "auth enabled: /health is exempt (no token needed)" do
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, @secret})

    resp = TimelessMetrics.TestHTTP.get(@port, "/health")

    assert resp.status == 200

    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, nil})
  end

  test "auth enabled: missing token returns 401" do
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, @secret})
    now = 1_700_000_000

    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/query?metric=cpu&from=#{now}&to=#{now + 60}")

    assert resp.status == 401
    assert :json.decode(resp.body)["error"] == "unauthorized"

    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, nil})
  end

  test "auth enabled: wrong token returns 403" do
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, @secret})
    now = 1_700_000_000

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/query?metric=cpu&from=#{now}&to=#{now + 60}",
        headers: [{"authorization", "Bearer wrong-token"}]
      )

    assert resp.status == 403
    assert :json.decode(resp.body)["error"] == "forbidden"

    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, nil})
  end

  test "auth enabled: valid token grants access to API" do
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, @secret})
    now = 1_700_000_000

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/query?metric=cpu&from=#{now}&to=#{now + 60}",
        headers: [{"authorization", "Bearer #{@secret}"}]
      )

    assert resp.status == 200

    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, nil})
  end

  test "auth enabled: token via query param grants access" do
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, @secret})
    now = 1_700_000_000

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/query?metric=cpu&from=#{now}&to=#{now + 60}&token=#{@secret}"
      )

    assert resp.status == 200

    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, nil})
  end

  test "auth enabled: wrong token via query param returns 403" do
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, @secret})
    now = 1_700_000_000

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/query?metric=cpu&from=#{now}&to=#{now + 60}&token=wrong"
      )

    assert resp.status == 403

    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, nil})
  end

  # --- Transforms ---

  test "transform: divide via query_range" do
    now = 1_700_000_000

    # Write values in tenths of dBmV
    TimelessMetrics.write(:http_test, "snr", %{"port" => "u0"}, 380.0, timestamp: now)
    TimelessMetrics.write(:http_test, "snr", %{"port" => "u0"}, 400.0, timestamp: now + 60)
    TimelessMetrics.flush(:http_test)

    # Query with divide:10 transform
    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/query_range?metric=snr&port=u0&from=#{now}&to=#{now + 120}&step=300&transform=divide:10"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    series = List.first(result["series"])
    [_ts, val] = List.first(series["data"])
    # avg of 380 and 400 = 390, divided by 10 = 39.0
    assert_in_delta val, 39.0, 0.01
  end

  test "transform: multiply via query_range" do
    now = 1_700_000_000

    TimelessMetrics.write(:http_test, "ratio", %{"id" => "1"}, 0.95, timestamp: now)
    TimelessMetrics.flush(:http_test)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/query_range?metric=ratio&id=1&from=#{now}&to=#{now + 60}&step=300&transform=multiply:100"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    series = List.first(result["series"])
    [_ts, val] = List.first(series["data"])
    assert_in_delta val, 95.0, 0.01
  end

  test "transform: works on chart endpoint" do
    now = 1_700_000_000

    TimelessMetrics.write(:http_test, "snr_chart", %{"port" => "u0"}, 380.0, timestamp: now)
    TimelessMetrics.write(:http_test, "snr_chart", %{"port" => "u0"}, 400.0, timestamp: now + 300)
    TimelessMetrics.flush(:http_test)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/chart?metric=snr_chart&port=u0&from=#{now}&to=#{now + 600}&transform=divide:10"
      )

    assert resp.status == 200
    assert {"content-type", "image/svg+xml"} in resp.headers
  end

  test "transform: no transform when param absent" do
    now = 1_700_000_000

    TimelessMetrics.write(:http_test, "raw_val", %{"id" => "1"}, 42.0, timestamp: now)
    TimelessMetrics.flush(:http_test)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/query_range?metric=raw_val&id=1&from=#{now}&to=#{now + 60}&step=300"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    series = List.first(result["series"])
    [_ts, val] = List.first(series["data"])
    assert_in_delta val, 42.0, 0.01
  end

  test "auth enabled: POST endpoint requires token" do
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, @secret})

    lines =
      :json.encode(%{
        metric: %{__name__: "cpu", host: "web-1"},
        values: [1.0],
        timestamps: [1_700_000_000]
      })
      |> IO.iodata_to_binary()

    resp = TimelessMetrics.TestHTTP.post(@port, "/api/v1/import", lines)

    assert resp.status == 401
    assert TimelessMetrics.TestHTTP.post(@port, "/api/v1/flush", "").status == 401

    resp =
      TimelessMetrics.TestHTTP.post(@port, "/api/v1/import", lines,
        headers: [{"authorization", "Bearer #{@secret}"}]
      )

    assert resp.status == 204

    flush =
      TimelessMetrics.TestHTTP.post(@port, "/api/v1/flush", "",
        headers: [{"authorization", "Bearer #{@secret}"}]
      )

    assert flush.status == 200

    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:http_test, nil})
  end

  # --- Forecast endpoint ---

  test "GET /api/v1/forecast returns predictions" do
    now = System.os_time(:second)

    # Need enough points for forecast model (3 + 2*periods minimum)
    # 100 points at 5-min intervals
    for i <- 0..99 do
      ts = now - (99 - i) * 300
      val = 50.0 + 20.0 * :math.sin(2 * :math.pi() * i / 50)
      TimelessMetrics.write(:http_test, "forecast_test", %{"host" => "a"}, val, timestamp: ts)
    end

    TimelessMetrics.flush(:http_test)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/forecast?metric=forecast_test&host=a&from=#{now - 100 * 300}&to=#{now}&step=300&horizon=3600"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["metric"] == "forecast_test"
    assert length(result["series"]) >= 1

    series = List.first(result["series"])
    assert is_list(series["data"])
    assert is_list(series["forecast"])
    assert length(series["forecast"]) > 0
  end

  test "GET /api/v1/forecast with insufficient data returns empty forecast" do
    now = System.os_time(:second)

    TimelessMetrics.write(:http_test, "sparse_fc", %{"id" => "1"}, 42.0, timestamp: now)
    TimelessMetrics.flush(:http_test)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/forecast?metric=sparse_fc&id=1&from=#{now - 60}&to=#{now + 60}&step=300&horizon=3600"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    series = List.first(result["series"])
    assert series["forecast"] == []
  end

  # --- Anomaly endpoint ---

  test "GET /api/v1/anomalies returns anomaly analysis" do
    now = System.os_time(:second)

    # Write smooth data with one spike
    for i <- 0..49 do
      ts = now - (49 - i) * 300
      val = if i == 25, do: 999.0, else: 50.0 + 5.0 * :math.sin(2 * :math.pi() * i / 25)
      TimelessMetrics.write(:http_test, "anom_test", %{"host" => "b"}, val, timestamp: ts)
    end

    TimelessMetrics.flush(:http_test)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/anomalies?metric=anom_test&host=b&from=#{now - 50 * 300}&to=#{now}&step=300&sensitivity=high"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["metric"] == "anom_test"
    assert length(result["series"]) >= 1

    series = List.first(result["series"])
    assert is_list(series["analysis"])

    # At least one anomaly should be flagged (the spike)
    anomalies = Enum.filter(series["analysis"], & &1["anomaly"])
    assert length(anomalies) >= 1
  end

  # --- Chart with forecast and anomaly overlays ---

  test "GET /chart with forecast overlay renders SVG" do
    now = System.os_time(:second)

    for i <- 0..99 do
      ts = now - (99 - i) * 300
      val = 50.0 + 20.0 * :math.sin(2 * :math.pi() * i / 50)
      TimelessMetrics.write(:http_test, "chart_fc", %{"id" => "1"}, val, timestamp: ts)
    end

    TimelessMetrics.flush(:http_test)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/chart?metric=chart_fc&id=1&from=#{now - 100 * 300}&to=#{now}&step=300&forecast=1h"
      )

    assert resp.status == 200
    assert resp.body =~ "<svg"
    # Forecast line should be rendered as a dashed polyline
    assert resp.body =~ "stroke-dasharray"
  end

  test "GET /chart with anomaly dots renders SVG" do
    now = System.os_time(:second)

    for i <- 0..49 do
      ts = now - (49 - i) * 300
      val = if i == 25, do: 999.0, else: 50.0 + 5.0 * :math.sin(2 * :math.pi() * i / 25)
      TimelessMetrics.write(:http_test, "chart_anom", %{"id" => "1"}, val, timestamp: ts)
    end

    TimelessMetrics.flush(:http_test)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/chart?metric=chart_anom&id=1&from=#{now - 50 * 300}&to=#{now}&step=300&anomalies=high"
      )

    assert resp.status == 200
    assert resp.body =~ "<svg"
    # Anomaly points rendered as circles
    assert resp.body =~ "<circle"
  end

  # --- Annotations endpoint ---

  test "POST /api/v1/annotations creates annotation" do
    now = System.os_time(:second)

    body =
      :json.encode(%{
        title: "Deploy v1.2.3",
        description: "Production rollout",
        timestamp: now,
        tags: ["deploy", "production"]
      })
      |> IO.iodata_to_binary()

    resp = TimelessMetrics.TestHTTP.post(@port, "/api/v1/annotations", body)

    assert resp.status == 201
    result = :json.decode(resp.body)
    assert result["status"] == "created"
    assert is_integer(result["id"])

    # Verify annotation was stored
    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/annotations?from=#{now - 60}&to=#{now + 60}")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert length(result["data"]) == 1
    annot = List.first(result["data"])
    assert annot["title"] == "Deploy v1.2.3"
  end

  test "POST /api/v1/annotations rejects missing title" do
    body = :json.encode(%{description: "no title"}) |> IO.iodata_to_binary()

    resp = TimelessMetrics.TestHTTP.post(@port, "/api/v1/annotations", body)

    assert resp.status == 400
  end

  test "GET /api/v1/status/buildinfo identifies as Prometheus-compatible" do
    for path <- ["/api/v1/status/buildinfo", "/prometheus/api/v1/status/buildinfo"] do
      resp = TimelessMetrics.TestHTTP.get(@port, path)
      assert resp.status == 200
      body = :json.decode(resp.body)
      assert body["status"] == "success"
      assert is_binary(body["data"]["version"])
      assert body["data"]["application"] == "timeless-metrics"
    end
  end

  test "GET /api/v1/status/config returns an empty config" do
    resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/status/config")
    assert resp.status == 200
    assert :json.decode(resp.body)["data"]["yaml"] == ""
  end

  test "series endpoint unions repeated match[] params" do
    now = System.os_time(:second)
    TimelessMetrics.write(:http_test, "m_one", %{"host" => "a"}, 1.0, timestamp: now)
    TimelessMetrics.write(:http_test, "m_two", %{"host" => "b"}, 2.0, timestamp: now)
    TimelessMetrics.flush(:http_test)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/prometheus/api/v1/series?match[]=m_one&match[]=m_two"
      )

    assert resp.status == 200
    data = :json.decode(resp.body)["data"]
    names = data |> Enum.map(& &1["__name__"]) |> Enum.sort()
    assert names == ["m_one", "m_two"]
  end

  test "labels and label-values endpoints honor match[] selectors" do
    now = System.os_time(:second)

    TimelessMetrics.write(:http_test, "lm_a", %{"dc" => "east", "app" => "web"}, 1.0,
      timestamp: now
    )

    TimelessMetrics.write(:http_test, "lm_b", %{"zone" => "z1"}, 2.0, timestamp: now)
    TimelessMetrics.flush(:http_test)

    resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/labels?match%5B%5D=lm_a")
    assert resp.status == 200
    names = :json.decode(resp.body)["data"]
    assert "dc" in names and "app" in names
    refute "zone" in names

    resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/label/dc/values?match%5B%5D=lm_a")
    assert :json.decode(resp.body)["data"] == ["east"]

    resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/label/dc/values?match%5B%5D=lm_b")
    assert :json.decode(resp.body)["data"] == []

    # native series endpoint accepts match[] selectors too
    resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/series?match%5B%5D=lm_a&match%5B%5D=lm_b")

    names =
      :json.decode(resp.body)["data"] |> Enum.map(& &1["__name__"]) |> Enum.sort()

    assert names == ["lm_a", "lm_b"]
  end

  test "rejected PromQL queries land in the gap radar" do
    now = System.os_time(:second)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/query_range?query=#{URI.encode_www_form("mad_over_time(foo[5m])")}&start=#{now - 60}&end=#{now}&step=60"
      )

    assert resp.status == 400
    assert :json.decode(resp.body)["status"] == "error"

    health = TimelessMetrics.TestHTTP.get(@port, "/health/detailed")
    assert health.status == 200
    detailed = :json.decode(health.body)

    assert detailed["promql_rejected"] >= 1

    assert Enum.any?(detailed["promql_rejections"], fn r ->
             r["query"] =~ "mad_over_time" and r["reason"] =~ "not supported"
           end)
  end
end
