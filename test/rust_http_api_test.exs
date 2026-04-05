defmodule TimelessMetrics.RustHTTPAPITest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper
  alias TimelessMetrics.RustEngine
  alias TimelessMetrics.RustEngine.Nif

  @data_dir "/tmp/timeless_rust_http_test_#{System.os_time(:millisecond)}"
  @port 18_451

  setup_all do
    start_supervised!({TimelessMetrics.HTTP, store: :rust_http_test, port: @port})
    Process.sleep(50)

    on_exit(fn ->
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:rust_http_test, nil})
    end)

    :ok
  end

  setup do
    TestHelper.await_down(:rust_http_test_sup)
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:rust_http_test, nil})

    start_supervised!(
      {TimelessMetrics, name: :rust_http_test, data_dir: @data_dir, engine: :rust}
    )

    on_exit(fn ->
      TestHelper.await_down(:rust_http_test_sup)
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:rust_http_test, nil})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "GET /api/v1/query returns latest value with rust engine" do
    now = System.os_time(:second)
    base = now - 2
    seed_points(:rust_http_test, "mem_usage", %{"host" => "db-1"}, base, [40.0, 45.0, 50.0])

    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/query?metric=mem_usage&host=db-1")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["timestamp"] == now
    assert_in_delta result["value"], 50.0, 0.01
  end

  test "GET /api/v1/query_range returns bucketed aggregation with rust engine" do
    now = System.os_time(:second)
    seed_points(:rust_http_test, "req_rate", %{"svc" => "api"}, now, Enum.map(0..9, &(&1 * 10.0)))

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/query_range?metric=req_rate&svc=api&from=#{now - 1}&to=#{now + 10}&step=5&aggregate=avg"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["metric"] == "req_rate"
    assert length(result["series"]) == 1

    series = List.first(result["series"])
    assert series["labels"] == %{"svc" => "api"}
    assert length(series["data"]) >= 2
  end

  test "GET /prometheus/api/v1/query_range returns matrix format with rust engine" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:rust_http_test, "cpu_usage", %{"host" => "web-1"}, 50.0 + i,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:rust_http_test)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/prometheus/api/v1/query_range?query=cpu_usage&start=#{base}&end=#{base + 360}&step=60"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["status"] == "success"
    assert result["data"]["resultType"] == "matrix"
    assert length(result["data"]["result"]) >= 1
  end

  test "GET /prometheus/api/v1/query returns vector format with rust engine" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..4 do
      TimelessMetrics.write(:rust_http_test, "mem_usage", %{"host" => "web-1"}, 100.0 + i,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:rust_http_test)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/prometheus/api/v1/query?query=mem_usage&time=#{base + 300}"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["status"] == "success"
    assert result["data"]["resultType"] == "vector"
    assert length(result["data"]["result"]) >= 1

    first = List.first(result["data"]["result"])
    assert first["metric"]["__name__"] == "mem_usage"
    assert first["metric"]["host"] == "web-1"
    [ts, val_str] = first["value"]
    assert is_integer(ts)
    assert is_binary(val_str)
  end

  test "resolve_series and write_resolved work with rust engine" do
    now = System.os_time(:second)
    labels = %{"host" => "resolver-1"}

    {:ok, series_id} = TimelessMetrics.resolve_series(:rust_http_test, "resolved_metric", labels)
    assert is_integer(series_id)

    :ok = TimelessMetrics.write_resolved(:rust_http_test, series_id, 12.5, timestamp: now)
    :ok = TimelessMetrics.write_resolved(:rust_http_test, series_id, 15.0, timestamp: now + 1)
    TimelessMetrics.flush(:rust_http_test)

    {:ok, points} =
      TimelessMetrics.query(:rust_http_test, "resolved_metric", labels,
        from: now - 1,
        to: now + 5
      )

    assert points == [{now, 12.5}, {now + 1, 15.0}]
  end

  test "info returns stats with rust engine" do
    now = System.os_time(:second)

    TimelessMetrics.write(:rust_http_test, "info_metric", %{"host" => "web-1"}, 10.0,
      timestamp: now
    )

    TimelessMetrics.write(:rust_http_test, "info_metric", %{"host" => "web-2"}, 20.0,
      timestamp: now + 1
    )

    TimelessMetrics.flush(:rust_http_test)

    info = TimelessMetrics.info(:rust_http_test)

    assert info.series_count == 2
    assert info.total_points == 2
    assert info.disk_points >= 1
    assert info.storage_bytes > 0
    assert is_binary(info.db_path)
  end

  test "engine_info returns wrapped success payload" do
    now = System.os_time(:second)

    TimelessMetrics.write(:rust_http_test, "nif_info_metric", %{"host" => "web-1"}, 10.0,
      timestamp: now
    )

    TimelessMetrics.flush(:rust_http_test)

    assert {:ok, raw} = Nif.engine_info(RustEngine.ref(:rust_http_test))
    assert is_map(raw)
    assert raw["series_count"] == 1
    assert raw["total_points"] == 1
    assert is_number(raw["buffer_memory_bytes"])
  end

  test "query_aggregate_multi returns scalar aggregates without fetching raw points" do
    now = System.os_time(:second)

    TimelessMetrics.write(:rust_http_test, "agg_multi", %{"host" => "web-1"}, 10.0,
      timestamp: now
    )

    TimelessMetrics.write(:rust_http_test, "agg_multi", %{"host" => "web-1"}, 20.0,
      timestamp: now + 1
    )

    TimelessMetrics.write(:rust_http_test, "agg_multi", %{"host" => "web-2"}, 30.0,
      timestamp: now
    )

    TimelessMetrics.write(:rust_http_test, "agg_multi", %{"host" => "web-2"}, 50.0,
      timestamp: now + 1
    )

    TimelessMetrics.flush(:rust_http_test)

    {:ok, results} =
      TimelessMetrics.query_aggregate_multi(:rust_http_test, "agg_multi", %{},
        from: now - 1,
        to: now + 5,
        aggregate: :avg
      )

    assert length(results) == 2

    sorted =
      results
      |> Enum.sort_by(& &1.labels["host"])

    assert sorted == [
             %{labels: %{"host" => "web-1"}, data: [{now - 1, 15.0}]},
             %{labels: %{"host" => "web-2"}, data: [{now - 1, 40.0}]}
           ]
  end

  test "query_aggregate_multi returns per-second rates for bucketed counter queries with rust engine" do
    now = System.os_time(:second)
    base = div(now, 300) * 300

    for i <- 0..5 do
      TimelessMetrics.write(:rust_http_test, "octets_in", %{"host" => "ap-1"}, i * 120.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:rust_http_test)

    {:ok, [%{labels: %{"host" => "ap-1"}, data: data}]} =
      TimelessMetrics.query_aggregate_multi(:rust_http_test, "octets_in", %{"host" => "ap-1"},
        from: base,
        to: base + 360,
        bucket: {60, :seconds},
        aggregate: :rate
      )

    assert length(data) >= 4

    Enum.each(data, fn {_ts, rate} ->
      assert_in_delta rate, 120.0 / 60, 0.1
    end)

    {:ok, [%{data: last_data}]} =
      TimelessMetrics.query_aggregate_multi(:rust_http_test, "octets_in", %{"host" => "ap-1"},
        from: base,
        to: base + 360,
        bucket: {60, :seconds},
        aggregate: :last
      )

    refute data == last_data
  end

  test "query_aggregate_multi supports every bucketed aggregate with rust engine" do
    base = div(System.os_time(:second), 180) * 180

    points = [
      {base + 0, 10.0},
      {base + 20, 20.0},
      {base + 40, 30.0},
      {base + 60, 40.0},
      {base + 80, 50.0},
      {base + 100, 70.0},
      {base + 120, 80.0},
      {base + 140, 90.0},
      {base + 160, 120.0}
    ]

    Enum.each(points, fn {ts, val} ->
      TimelessMetrics.write(:rust_http_test, "agg_coverage", %{"host" => "agg-1"}, val,
        timestamp: ts
      )
    end)

    TimelessMetrics.flush(:rust_http_test)

    assert_multi_aggregate(:avg, [{base, 20.0}, {base + 60, 160.0 / 3}, {base + 120, 290.0 / 3}])
    assert_multi_aggregate(:min, [{base, 10.0}, {base + 60, 40.0}, {base + 120, 80.0}])
    assert_multi_aggregate(:max, [{base, 30.0}, {base + 60, 70.0}, {base + 120, 120.0}])
    assert_multi_aggregate(:sum, [{base, 60.0}, {base + 60, 160.0}, {base + 120, 290.0}])
    assert_multi_aggregate(:count, [{base, 3.0}, {base + 60, 3.0}, {base + 120, 3.0}])
    assert_multi_aggregate(:last, [{base, 30.0}, {base + 60, 70.0}, {base + 120, 120.0}])
    assert_multi_aggregate(:first, [{base, 10.0}, {base + 60, 40.0}, {base + 120, 80.0}])

    {:ok, [%{labels: %{"host" => "agg-1"}, data: rate_data}]} =
      TimelessMetrics.query_aggregate_multi(:rust_http_test, "agg_coverage", %{"host" => "agg-1"},
        from: base,
        to: base + 180,
        bucket: {60, :seconds},
        aggregate: :rate
      )

    assert length(rate_data) == 2
    assert_in_delta elem(Enum.at(rate_data, 0), 1), 40.0 / 60, 0.0001
    assert_in_delta elem(Enum.at(rate_data, 1), 1), 50.0 / 60, 0.0001
  end

  test "GET /prometheus/api/v1/labels returns label names with rust engine" do
    now = System.os_time(:second)

    TimelessMetrics.write(
      :rust_http_test,
      "cpu_usage",
      %{"host" => "web-1", "region" => "us"},
      50.0,
      timestamp: now
    )

    TimelessMetrics.flush(:rust_http_test)

    resp = TimelessMetrics.TestHTTP.get(@port, "/prometheus/api/v1/labels")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["status"] == "success"
    assert "__name__" in result["data"]
    assert "host" in result["data"]
    assert "region" in result["data"]
  end

  defp assert_multi_aggregate(aggregate, expected_data) do
    base = elem(hd(expected_data), 0)
    to = elem(List.last(expected_data), 0) + 60

    {:ok, [%{labels: %{"host" => "agg-1"}, data: actual_data}]} =
      TimelessMetrics.query_aggregate_multi(:rust_http_test, "agg_coverage", %{"host" => "agg-1"},
        from: base,
        to: to,
        bucket: {60, :seconds},
        aggregate: aggregate
      )

    assert length(actual_data) == length(expected_data)

    Enum.zip(actual_data, expected_data)
    |> Enum.each(fn {{actual_ts, actual_val}, {expected_ts, expected_val}} ->
      assert actual_ts == expected_ts
      assert_in_delta actual_val, expected_val, 0.0001
    end)
  end

  test "GET /prometheus/api/v1/label/__name__/values returns metric names with rust engine" do
    now = System.os_time(:second)
    TimelessMetrics.write(:rust_http_test, "cpu_usage", %{"host" => "a"}, 1.0, timestamp: now)
    TimelessMetrics.write(:rust_http_test, "mem_usage", %{"host" => "a"}, 2.0, timestamp: now)
    TimelessMetrics.flush(:rust_http_test)

    resp = TimelessMetrics.TestHTTP.get(@port, "/prometheus/api/v1/label/__name__/values")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["status"] == "success"
    assert "cpu_usage" in result["data"]
    assert "mem_usage" in result["data"]
  end

  test "GET /prometheus/api/v1/label/:name/values returns deduplicated values with rust engine" do
    now = System.os_time(:second)
    TimelessMetrics.write(:rust_http_test, "cpu", %{"host" => "web-1"}, 1.0, timestamp: now)
    TimelessMetrics.write(:rust_http_test, "cpu", %{"host" => "web-2"}, 2.0, timestamp: now)
    TimelessMetrics.write(:rust_http_test, "mem", %{"host" => "web-1"}, 3.0, timestamp: now)
    TimelessMetrics.flush(:rust_http_test)

    resp = TimelessMetrics.TestHTTP.get(@port, "/prometheus/api/v1/label/host/values")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["status"] == "success"
    assert result["data"] == Enum.sort(result["data"])
    assert "web-1" in result["data"]
    assert "web-2" in result["data"]
    assert length(Enum.filter(result["data"], &(&1 == "web-1"))) == 1
  end

  test "GET /prometheus/api/v1/series returns matching series with rust engine" do
    now = System.os_time(:second)

    TimelessMetrics.write(
      :rust_http_test,
      "cpu",
      %{"host" => "web-1", "region" => "us"},
      1.0,
      timestamp: now
    )

    TimelessMetrics.write(
      :rust_http_test,
      "cpu",
      %{"host" => "web-2", "region" => "eu"},
      2.0,
      timestamp: now
    )

    TimelessMetrics.flush(:rust_http_test)

    resp = TimelessMetrics.TestHTTP.get(@port, "/prometheus/api/v1/series?match[]=cpu")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["status"] == "success"
    assert length(result["data"]) == 2
    assert Enum.all?(result["data"], &(&1["__name__"] == "cpu"))
  end

  test "GET /prometheus/api/v1/series respects label filters with rust engine" do
    now = System.os_time(:second)
    TimelessMetrics.write(:rust_http_test, "cpu", %{"host" => "web-1"}, 1.0, timestamp: now)
    TimelessMetrics.write(:rust_http_test, "cpu", %{"host" => "web-2"}, 2.0, timestamp: now)
    TimelessMetrics.flush(:rust_http_test)

    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        ~s(/prometheus/api/v1/series?match[]=cpu%7Bhost%3D%22web-1%22%7D)
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert length(result["data"]) == 1
    assert List.first(result["data"])["host"] == "web-1"
  end

  defp seed_points(store, metric, labels, base_ts, values) do
    Enum.with_index(values)
    |> Enum.each(fn {value, offset} ->
      TimelessMetrics.write(store, metric, labels, value, timestamp: base_ts + offset)
    end)

    TimelessMetrics.flush(store)
  end
end
