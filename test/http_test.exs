defmodule Timeless.HTTPTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_http_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!({Timeless, name: :http_test, data_dir: @data_dir, buffer_shards: 1})

    on_exit(fn ->
      :persistent_term.erase({Timeless, :http_test, :schema})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "POST /api/v1/import ingests JSON lines" do
    now = 1_700_000_000

    lines =
      Enum.join(
        [
          Jason.encode!(%{
            metric: %{__name__: "cpu_usage", host: "web-1"},
            values: [73.2, 74.1],
            timestamps: [now, now + 60]
          }),
          Jason.encode!(%{
            metric: %{__name__: "mem_usage", host: "web-1"},
            values: [45.0],
            timestamps: [now]
          })
        ],
        "\n"
      )

    conn =
      Plug.Test.conn(:post, "/api/v1/import", lines)
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 204

    Timeless.flush(:http_test)

    {:ok, cpu_points} =
      Timeless.query(:http_test, "cpu_usage", %{"host" => "web-1"},
        from: now - 60,
        to: now + 120
      )

    assert length(cpu_points) == 2
    assert {^now, v1} = List.first(cpu_points)
    assert_in_delta v1, 73.2, 0.01

    {:ok, mem_points} =
      Timeless.query(:http_test, "mem_usage", %{"host" => "web-1"},
        from: now - 60,
        to: now + 60
      )

    assert [{^now, 45.0}] = mem_points
  end

  test "POST /api/v1/import handles multiple series in batch" do
    now = 1_700_000_000

    lines =
      for i <- 1..50 do
        Jason.encode!(%{
          metric: %{__name__: "cpu", host: "host-#{i}"},
          values: [i * 1.0],
          timestamps: [now]
        })
      end
      |> Enum.join("\n")

    conn =
      Plug.Test.conn(:post, "/api/v1/import", lines)
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 204

    Timeless.flush(:http_test)

    info = Timeless.info(:http_test)
    assert info.series_count == 50
  end

  test "POST /api/v1/import reports partial errors" do
    body = ~s|{"metric":{"__name__":"good"},"values":[1.0],"timestamps":[1700000000]}
this is not json
{"metric":{"__name__":"also_good"},"values":[2.0],"timestamps":[1700000001]}|

    conn =
      Plug.Test.conn(:post, "/api/v1/import", body)
      |> Timeless.HTTP.call(store: :http_test)

    # Returns 200 with error count when there are partial failures
    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert result["samples"] == 2
    assert result["errors"] == 1
  end

  test "POST /api/v1/import with empty body" do
    conn =
      Plug.Test.conn(:post, "/api/v1/import", "")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 204
  end

  test "POST /api/v1/import rejects mismatched array lengths" do
    body =
      Jason.encode!(%{
        metric: %{__name__: "bad"},
        values: [1.0, 2.0],
        timestamps: [1_700_000_000]
      })

    conn =
      Plug.Test.conn(:post, "/api/v1/import", body)
      |> Timeless.HTTP.call(store: :http_test)

    # Entire line treated as error
    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert result["errors"] == 1
    assert result["samples"] == 0
  end

  test "GET /health returns store stats" do
    conn =
      Plug.Test.conn(:get, "/health")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)
    assert body["status"] == "ok"
    assert is_integer(body["series"])
    assert is_integer(body["points"])
    assert is_integer(body["storage_bytes"])
  end

  test "unknown route returns 404" do
    conn =
      Plug.Test.conn(:get, "/nonexistent")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 404
  end

  # --- Query endpoints ---

  test "GET /api/v1/export returns raw points in VM format" do
    now = 1_700_000_000
    seed_points(:http_test, "cpu_usage", %{"host" => "web-1"}, now, [73.2, 74.1, 75.0])

    conn =
      Plug.Test.conn(:get, "/api/v1/export?metric=cpu_usage&host=web-1&from=#{now - 60}&to=#{now + 120}")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert result["metric"]["__name__"] == "cpu_usage"
    assert result["metric"]["host"] == "web-1"
    assert length(result["values"]) == 3
    assert length(result["timestamps"]) == 3
  end

  test "GET /api/v1/export returns empty body for no data" do
    conn =
      Plug.Test.conn(:get, "/api/v1/export?metric=nonexistent&from=0&to=9999999999")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    assert conn.resp_body == ""
  end

  test "GET /api/v1/export requires metric param" do
    conn =
      Plug.Test.conn(:get, "/api/v1/export?from=0&to=9999999999")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 400
    assert Jason.decode!(conn.resp_body)["error"] =~ "metric"
  end

  test "GET /api/v1/query returns latest value" do
    now = 1_700_000_000
    seed_points(:http_test, "mem_usage", %{"host" => "db-1"}, now, [40.0, 45.0, 50.0])

    conn =
      Plug.Test.conn(:get, "/api/v1/query?metric=mem_usage&host=db-1")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert result["timestamp"] == now + 2
    assert_in_delta result["value"], 50.0, 0.01
  end

  test "GET /api/v1/query returns null for missing series" do
    conn =
      Plug.Test.conn(:get, "/api/v1/query?metric=nonexistent")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert result["timestamp"] == nil
    assert result["value"] == nil
  end

  test "GET /api/v1/query_range returns bucketed aggregation" do
    # Use current time so points fall within raw retention window
    now = System.os_time(:second)

    # Write 10 points, 1 per second
    values = for i <- 0..9, do: i * 10.0
    seed_points(:http_test, "req_rate", %{"svc" => "api"}, now, values)

    conn =
      Plug.Test.conn(
        :get,
        "/api/v1/query_range?metric=req_rate&svc=api&from=#{now - 1}&to=#{now + 10}&step=5&aggregate=avg"
      )
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
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
      conn =
        Plug.Test.conn(
          :get,
          "/api/v1/query_range?metric=latency&ep=/health&from=#{now - 1}&to=#{now + 10}&step=3600&aggregate=#{agg}"
        )
        |> Timeless.HTTP.call(store: :http_test)

      assert conn.status == 200
      result = Jason.decode!(conn.resp_body)
      series = result["series"]
      assert length(series) >= 1, "aggregate #{agg} returned no series"
      assert length(List.first(series)["data"]) >= 1, "aggregate #{agg} returned no data"
    end
  end

  test "GET /api/v1/query_range requires metric param" do
    conn =
      Plug.Test.conn(:get, "/api/v1/query_range?from=0&to=9999999999&step=60")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 400
  end

  # --- Multi-series label filtering ---

  test "GET /api/v1/export with partial labels returns multiple series" do
    now = 1_700_000_000
    seed_points(:http_test, "cpu", %{"host" => "web-1", "dc" => "us"}, now, [10.0])
    seed_points(:http_test, "cpu", %{"host" => "web-2", "dc" => "us"}, now, [20.0])
    seed_points(:http_test, "cpu", %{"host" => "db-1", "dc" => "eu"}, now, [30.0])

    # Filter by dc=us only — should match 2 series
    conn =
      Plug.Test.conn(:get, "/api/v1/export?metric=cpu&dc=us&from=#{now - 60}&to=#{now + 60}")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    lines = String.split(conn.resp_body, "\n", trim: true)
    assert length(lines) == 2
  end

  test "GET /api/v1/export with no labels returns all series" do
    now = 1_700_000_000
    seed_points(:http_test, "mem", %{"host" => "a"}, now, [1.0])
    seed_points(:http_test, "mem", %{"host" => "b"}, now, [2.0])
    seed_points(:http_test, "mem", %{"host" => "c"}, now, [3.0])

    conn =
      Plug.Test.conn(:get, "/api/v1/export?metric=mem&from=#{now - 60}&to=#{now + 60}")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    lines = String.split(conn.resp_body, "\n", trim: true)
    assert length(lines) == 3
  end

  test "GET /api/v1/query_range with partial labels returns multiple series" do
    now = System.os_time(:second)
    seed_points(:http_test, "req", %{"svc" => "api", "method" => "GET"}, now, [1.0, 2.0])
    seed_points(:http_test, "req", %{"svc" => "api", "method" => "POST"}, now, [3.0, 4.0])
    seed_points(:http_test, "req", %{"svc" => "web", "method" => "GET"}, now, [5.0, 6.0])

    # Filter by svc=api — should match 2 series
    conn =
      Plug.Test.conn(
        :get,
        "/api/v1/query_range?metric=req&svc=api&from=#{now - 1}&to=#{now + 10}&step=3600&aggregate=avg"
      )
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert length(result["series"]) == 2
  end

  test "Elixir API query_multi with label filter" do
    now = System.os_time(:second)
    seed_points(:http_test, "disk", %{"host" => "a", "mount" => "/"}, now, [50.0])
    seed_points(:http_test, "disk", %{"host" => "a", "mount" => "/data"}, now, [80.0])
    seed_points(:http_test, "disk", %{"host" => "b", "mount" => "/"}, now, [30.0])

    # Filter host=a — 2 series
    {:ok, results} =
      Timeless.query_multi(:http_test, "disk", %{"host" => "a"},
        from: now - 60,
        to: now + 60
      )

    assert length(results) == 2
    assert Enum.all?(results, fn %{labels: l} -> l["host"] == "a" end)

    # No filter — all 3 series
    {:ok, all_results} =
      Timeless.query_multi(:http_test, "disk", %{},
        from: now - 60,
        to: now + 60
      )

    assert length(all_results) == 3
  end

  # --- Chart endpoint ---

  test "GET /chart returns SVG image" do
    now = System.os_time(:second)
    seed_points(:http_test, "chart_test", %{"host" => "web-1"}, now, [10.0, 20.0, 30.0, 25.0, 15.0])

    conn =
      Plug.Test.conn(:get, "/chart?metric=chart_test&host=web-1&from=#{now - 1}&to=#{now + 10}&step=1")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    assert {"content-type", "image/svg+xml; charset=utf-8"} in conn.resp_headers
    assert conn.resp_body =~ "<svg"
    assert conn.resp_body =~ "chart_test"
    assert conn.resp_body =~ "<polyline"
  end

  test "GET /chart with multiple series renders multi-line chart" do
    now = System.os_time(:second)
    seed_points(:http_test, "multi_chart", %{"host" => "a"}, now, [10.0, 20.0, 30.0])
    seed_points(:http_test, "multi_chart", %{"host" => "b"}, now, [30.0, 20.0, 10.0])

    conn =
      Plug.Test.conn(:get, "/chart?metric=multi_chart&from=#{now - 1}&to=#{now + 10}&step=1")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    assert conn.resp_body =~ "<svg"
    # Should have 2 polylines (one per series)
    assert length(Regex.scan(~r/<polyline/, conn.resp_body)) == 2
  end

  test "GET /chart with custom dimensions" do
    now = System.os_time(:second)
    seed_points(:http_test, "sized_chart", %{"id" => "1"}, now, [1.0, 2.0, 3.0])

    conn =
      Plug.Test.conn(:get, "/chart?metric=sized_chart&id=1&from=#{now - 1}&to=#{now + 10}&width=400&height=200&step=1")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    assert conn.resp_body =~ ~s(width="400")
    assert conn.resp_body =~ ~s(height="200")
  end

  test "GET /chart with no data renders empty state" do
    conn =
      Plug.Test.conn(:get, "/chart?metric=nonexistent&from=0&to=9999999999&step=60")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    assert conn.resp_body =~ "No data"
  end

  test "GET /chart supports relative time" do
    now = System.os_time(:second)
    seed_points(:http_test, "rel_chart", %{"id" => "1"}, now - 1800, [1.0, 2.0, 3.0])

    conn =
      Plug.Test.conn(:get, "/chart?metric=rel_chart&id=1&from=-1h&to=now")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    assert conn.resp_body =~ "<polyline"
  end

  test "GET /chart requires metric param" do
    conn =
      Plug.Test.conn(:get, "/chart?from=-1h")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 400
  end

  # --- Discovery endpoints ---

  test "GET /api/v1/label/__name__/values lists metric names" do
    now = System.os_time(:second)
    seed_points(:http_test, "cpu_usage", %{"host" => "a"}, now, [1.0])
    seed_points(:http_test, "mem_usage", %{"host" => "a"}, now, [2.0])
    seed_points(:http_test, "disk_io", %{"host" => "a"}, now, [3.0])

    conn =
      Plug.Test.conn(:get, "/api/v1/label/__name__/values")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
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

    conn =
      Plug.Test.conn(:get, "/api/v1/label/host/values?metric=cpu")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
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

    conn =
      Plug.Test.conn(:get, "/api/v1/series?metric=net_rx")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert length(result["data"]) == 3
  end

  test "discovery endpoints require metric param where needed" do
    conn =
      Plug.Test.conn(:get, "/api/v1/label/host/values")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 400

    conn =
      Plug.Test.conn(:get, "/api/v1/series")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 400
  end

  # --- Helpers ---

  defp seed_points(store, metric, labels, base_ts, values) do
    values
    |> Enum.with_index()
    |> Enum.each(fn {val, i} ->
      Timeless.write(store, metric, labels, val, timestamp: base_ts + i)
    end)

    Timeless.flush(store)
  end

  test "labels without __name__ default to 'unknown'" do
    body =
      Jason.encode!(%{
        metric: %{host: "web-1"},
        values: [99.0],
        timestamps: [1_700_000_000]
      })

    conn =
      Plug.Test.conn(:post, "/api/v1/import", body)
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 204

    Timeless.flush(:http_test)

    {:ok, points} =
      Timeless.query(:http_test, "unknown", %{"host" => "web-1"},
        from: 1_699_999_900,
        to: 1_700_000_100
      )

    assert length(points) == 1
  end

  # --- Bearer Token Auth ---

  @secret "test-secret-token"

  defp authed_call(conn, opts \\ []) do
    Timeless.HTTP.call(conn, Keyword.merge([store: :http_test, bearer_token: @secret], opts))
  end

  test "auth disabled: requests work without token" do
    conn =
      Plug.Test.conn(:get, "/health")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
  end

  test "auth enabled: valid Bearer header returns 200" do
    conn =
      Plug.Test.conn(:get, "/health")
      |> Plug.Conn.put_req_header("authorization", "Bearer #{@secret}")
      |> authed_call()

    assert conn.status == 200
  end

  test "auth enabled: /health is exempt (no token needed)" do
    conn =
      Plug.Test.conn(:get, "/health")
      |> authed_call()

    assert conn.status == 200
  end

  test "auth enabled: missing token returns 401" do
    now = 1_700_000_000

    conn =
      Plug.Test.conn(:get, "/api/v1/query?metric=cpu&from=#{now}&to=#{now + 60}")
      |> authed_call()

    assert conn.status == 401
    assert Jason.decode!(conn.resp_body)["error"] == "unauthorized"
  end

  test "auth enabled: wrong token returns 403" do
    now = 1_700_000_000

    conn =
      Plug.Test.conn(:get, "/api/v1/query?metric=cpu&from=#{now}&to=#{now + 60}")
      |> Plug.Conn.put_req_header("authorization", "Bearer wrong-token")
      |> authed_call()

    assert conn.status == 403
    assert Jason.decode!(conn.resp_body)["error"] == "forbidden"
  end

  test "auth enabled: valid token grants access to API" do
    now = 1_700_000_000

    conn =
      Plug.Test.conn(:get, "/api/v1/query?metric=cpu&from=#{now}&to=#{now + 60}")
      |> Plug.Conn.put_req_header("authorization", "Bearer #{@secret}")
      |> authed_call()

    assert conn.status == 200
  end

  test "auth enabled: token via query param grants access" do
    now = 1_700_000_000

    conn =
      Plug.Test.conn(:get, "/api/v1/query?metric=cpu&from=#{now}&to=#{now + 60}&token=#{@secret}")
      |> authed_call()

    assert conn.status == 200
  end

  test "auth enabled: wrong token via query param returns 403" do
    now = 1_700_000_000

    conn =
      Plug.Test.conn(:get, "/api/v1/query?metric=cpu&from=#{now}&to=#{now + 60}&token=wrong")
      |> authed_call()

    assert conn.status == 403
  end

  # --- Transforms ---

  test "transform: divide via query_range" do
    now = 1_700_000_000

    # Write values in tenths of dBmV
    Timeless.write(:http_test, "snr", %{"port" => "u0"}, 380.0, timestamp: now)
    Timeless.write(:http_test, "snr", %{"port" => "u0"}, 400.0, timestamp: now + 60)
    Timeless.flush(:http_test)

    # Query with divide:10 transform
    conn =
      Plug.Test.conn(:get, "/api/v1/query_range?metric=snr&port=u0&from=#{now}&to=#{now + 120}&step=300&transform=divide:10")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    series = List.first(result["series"])
    [_ts, val] = List.first(series["data"])
    # avg of 380 and 400 = 390, divided by 10 = 39.0
    assert_in_delta val, 39.0, 0.01
  end

  test "transform: multiply via query_range" do
    now = 1_700_000_000

    Timeless.write(:http_test, "ratio", %{"id" => "1"}, 0.95, timestamp: now)
    Timeless.flush(:http_test)

    conn =
      Plug.Test.conn(:get, "/api/v1/query_range?metric=ratio&id=1&from=#{now}&to=#{now + 60}&step=300&transform=multiply:100")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    series = List.first(result["series"])
    [_ts, val] = List.first(series["data"])
    assert_in_delta val, 95.0, 0.01
  end

  test "transform: works on chart endpoint" do
    now = 1_700_000_000

    Timeless.write(:http_test, "snr_chart", %{"port" => "u0"}, 380.0, timestamp: now)
    Timeless.write(:http_test, "snr_chart", %{"port" => "u0"}, 400.0, timestamp: now + 300)
    Timeless.flush(:http_test)

    conn =
      Plug.Test.conn(:get, "/chart?metric=snr_chart&port=u0&from=#{now}&to=#{now + 600}&transform=divide:10")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    assert {"content-type", "image/svg+xml; charset=utf-8"} in conn.resp_headers
  end

  test "transform: no transform when param absent" do
    now = 1_700_000_000

    Timeless.write(:http_test, "raw_val", %{"id" => "1"}, 42.0, timestamp: now)
    Timeless.flush(:http_test)

    conn =
      Plug.Test.conn(:get, "/api/v1/query_range?metric=raw_val&id=1&from=#{now}&to=#{now + 60}&step=300")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    series = List.first(result["series"])
    [_ts, val] = List.first(series["data"])
    assert_in_delta val, 42.0, 0.01
  end

  test "auth enabled: POST endpoint requires token" do
    lines = Jason.encode!(%{
      metric: %{__name__: "cpu", host: "web-1"},
      values: [1.0],
      timestamps: [1_700_000_000]
    })

    conn =
      Plug.Test.conn(:post, "/api/v1/import", lines)
      |> authed_call()

    assert conn.status == 401

    conn =
      Plug.Test.conn(:post, "/api/v1/import", lines)
      |> Plug.Conn.put_req_header("authorization", "Bearer #{@secret}")
      |> authed_call()

    assert conn.status == 204
  end

  # --- Forecast endpoint ---

  test "GET /api/v1/forecast returns predictions" do
    now = System.os_time(:second)

    # Need enough points for forecast model (3 + 2*periods minimum)
    # 100 points at 5-min intervals
    for i <- 0..99 do
      ts = now - (99 - i) * 300
      val = 50.0 + 20.0 * :math.sin(2 * :math.pi() * i / 50)
      Timeless.write(:http_test, "forecast_test", %{"host" => "a"}, val, timestamp: ts)
    end

    Timeless.flush(:http_test)

    conn =
      Plug.Test.conn(
        :get,
        "/api/v1/forecast?metric=forecast_test&host=a&from=#{now - 100 * 300}&to=#{now}&step=300&horizon=3600"
      )
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert result["metric"] == "forecast_test"
    assert length(result["series"]) >= 1

    series = List.first(result["series"])
    assert is_list(series["data"])
    assert is_list(series["forecast"])
    assert length(series["forecast"]) > 0
  end

  test "GET /api/v1/forecast with insufficient data returns empty forecast" do
    now = System.os_time(:second)

    Timeless.write(:http_test, "sparse_fc", %{"id" => "1"}, 42.0, timestamp: now)
    Timeless.flush(:http_test)

    conn =
      Plug.Test.conn(
        :get,
        "/api/v1/forecast?metric=sparse_fc&id=1&from=#{now - 60}&to=#{now + 60}&step=300&horizon=3600"
      )
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
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
      Timeless.write(:http_test, "anom_test", %{"host" => "b"}, val, timestamp: ts)
    end

    Timeless.flush(:http_test)

    conn =
      Plug.Test.conn(
        :get,
        "/api/v1/anomalies?metric=anom_test&host=b&from=#{now - 50 * 300}&to=#{now}&step=300&sensitivity=high"
      )
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
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
      Timeless.write(:http_test, "chart_fc", %{"id" => "1"}, val, timestamp: ts)
    end

    Timeless.flush(:http_test)

    conn =
      Plug.Test.conn(
        :get,
        "/chart?metric=chart_fc&id=1&from=#{now - 100 * 300}&to=#{now}&step=300&forecast=1h"
      )
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    assert conn.resp_body =~ "<svg"
    # Forecast line should be rendered as a dashed polyline
    assert conn.resp_body =~ "stroke-dasharray"
  end

  test "GET /chart with anomaly dots renders SVG" do
    now = System.os_time(:second)

    for i <- 0..49 do
      ts = now - (49 - i) * 300
      val = if i == 25, do: 999.0, else: 50.0 + 5.0 * :math.sin(2 * :math.pi() * i / 25)
      Timeless.write(:http_test, "chart_anom", %{"id" => "1"}, val, timestamp: ts)
    end

    Timeless.flush(:http_test)

    conn =
      Plug.Test.conn(
        :get,
        "/chart?metric=chart_anom&id=1&from=#{now - 50 * 300}&to=#{now}&step=300&anomalies=high"
      )
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    assert conn.resp_body =~ "<svg"
    # Anomaly points rendered as circles
    assert conn.resp_body =~ "<circle"
  end

  # --- Annotations endpoint ---

  test "POST /api/v1/annotations creates annotation" do
    now = System.os_time(:second)

    body =
      Jason.encode!(%{
        title: "Deploy v1.2.3",
        description: "Production rollout",
        timestamp: now,
        tags: ["deploy", "production"]
      })

    conn =
      Plug.Test.conn(:post, "/api/v1/annotations", body)
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 201
    result = Jason.decode!(conn.resp_body)
    assert result["status"] == "created"
    assert is_integer(result["id"])

    # Verify annotation was stored
    conn =
      Plug.Test.conn(:get, "/api/v1/annotations?from=#{now - 60}&to=#{now + 60}")
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert length(result["data"]) == 1
    annot = List.first(result["data"])
    assert annot["title"] == "Deploy v1.2.3"
  end

  test "POST /api/v1/annotations rejects missing title" do
    body = Jason.encode!(%{description: "no title"})

    conn =
      Plug.Test.conn(:post, "/api/v1/annotations", body)
      |> Timeless.HTTP.call(store: :http_test)

    assert conn.status == 400
  end
end
