defmodule TimelessMetrics.MetadataRateTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_meta_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {TimelessMetrics, name: :meta_test, data_dir: @data_dir, engine: :actor}
    )

    on_exit(fn -> File.rm_rf!(@data_dir) end)

    :ok
  end

  # --- Metadata ---

  test "register and retrieve metric metadata" do
    TimelessMetrics.register_metric(:meta_test, "cpu_usage", :gauge,
      unit: "%",
      description: "CPU utilization"
    )

    {:ok, meta} = TimelessMetrics.get_metadata(:meta_test, "cpu_usage")

    assert meta.type == :gauge
    assert meta.unit == "%"
    assert meta.description == "CPU utilization"
  end

  test "get_metadata returns nil for unregistered metric" do
    {:ok, meta} = TimelessMetrics.get_metadata(:meta_test, "nonexistent")
    assert meta == nil
  end

  test "register_metric upserts" do
    TimelessMetrics.register_metric(:meta_test, "bytes_in", :counter, unit: "bytes")

    TimelessMetrics.register_metric(:meta_test, "bytes_in", :counter,
      unit: "bytes/s",
      description: "Inbound traffic"
    )

    {:ok, meta} = TimelessMetrics.get_metadata(:meta_test, "bytes_in")
    assert meta.unit == "bytes/s"
    assert meta.description == "Inbound traffic"
  end

  # --- Metadata via HTTP ---

  test "POST and GET /api/v1/metadata via HTTP" do
    conn =
      Plug.Test.conn(
        :post,
        "/api/v1/metadata",
        Jason.encode!(%{
          metric: "disk_usage",
          type: "gauge",
          unit: "%",
          description: "Disk utilization"
        })
      )
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> TimelessMetrics.HTTP.call(store: :meta_test)

    assert conn.status == 200

    conn =
      Plug.Test.conn(:get, "/api/v1/metadata?metric=disk_usage")
      |> TimelessMetrics.HTTP.call(store: :meta_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert result["type"] == "gauge"
    assert result["unit"] == "%"
  end

  test "POST /api/v1/metadata rejects invalid type" do
    conn =
      Plug.Test.conn(
        :post,
        "/api/v1/metadata",
        Jason.encode!(%{
          metric: "foo",
          type: "invalid"
        })
      )
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> TimelessMetrics.HTTP.call(store: :meta_test)

    assert conn.status == 400
  end

  test "GET /api/v1/metadata returns default gauge for unregistered metric" do
    conn =
      Plug.Test.conn(:get, "/api/v1/metadata?metric=unknown_metric")
      |> TimelessMetrics.HTTP.call(store: :meta_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert result["type"] == "gauge"
    assert result["unit"] == nil
  end

  # --- Rate Aggregate ---

  test "rate aggregate computes per-second rate from raw data" do
    now = System.os_time(:second)
    base = div(now, 300) * 300

    # Monotonically increasing counter: 0, 100, 200, 300, 400, 500
    # 100 units per 60 seconds = 1.667/s
    for i <- 0..5 do
      TimelessMetrics.write(:meta_test, "bytes_in", %{"if" => "eth0"}, i * 100.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:meta_test)

    {:ok, buckets} =
      TimelessMetrics.query_aggregate(:meta_test, "bytes_in", %{"if" => "eth0"},
        from: base,
        to: base + 360,
        bucket: {300, :seconds},
        aggregate: :rate
      )

    assert length(buckets) >= 1
    {_bucket, rate} = List.first(buckets)
    # 100 bytes per 60 seconds = 1.667 bytes/sec
    assert_in_delta rate, 100.0 / 60, 0.1
  end

  test "rate handles counter resets gracefully" do
    now = System.os_time(:second)
    base = div(now, 360) * 360

    # Counter: 100, 200, 300, 50 (reset!), 150, 250
    values = [100.0, 200.0, 300.0, 50.0, 150.0, 250.0]

    for {val, i} <- Enum.with_index(values) do
      TimelessMetrics.write(:meta_test, "counter_reset", %{"id" => "1"}, val,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:meta_test)

    {:ok, buckets} =
      TimelessMetrics.query_aggregate(:meta_test, "counter_reset", %{"id" => "1"},
        from: base,
        to: base + 360,
        bucket: {360, :seconds},
        aggregate: :rate
      )

    assert length(buckets) >= 1
    {_bucket, rate} = List.first(buckets)
    # Positive deltas: 100+100+100+100 = 400 over 4*60=240s (skipping the reset pair)
    # = 400/240 = 1.667/s
    assert rate > 0
    assert_in_delta rate, 400.0 / 240, 0.1
  end

  test "rate via HTTP query_range" do
    now = System.os_time(:second)
    base = div(now, 600) * 600

    for i <- 0..9 do
      TimelessMetrics.write(:meta_test, "http_rate", %{"host" => "web-1"}, i * 1000.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:meta_test)

    conn =
      Plug.Test.conn(
        :get,
        "/api/v1/query_range?metric=http_rate&host=web-1&from=#{base}&to=#{base + 600}&step=600&aggregate=rate"
      )
      |> TimelessMetrics.HTTP.call(store: :meta_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    series = List.first(result["series"])
    assert series != nil
    assert length(series["data"]) >= 1
    # Each bucket should have ~16.67/s rate (1000 per 60s)
    [_ts, rate_val] = List.first(series["data"])
    assert_in_delta rate_val, 1000.0 / 60, 1.0
  end

  test "rate with less than 2 points returns 0" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    TimelessMetrics.write(:meta_test, "single_point", %{"id" => "1"}, 42.0, timestamp: base)

    TimelessMetrics.flush(:meta_test)

    {:ok, buckets} =
      TimelessMetrics.query_aggregate(:meta_test, "single_point", %{"id" => "1"},
        from: base,
        to: base + 60,
        bucket: {60, :seconds},
        aggregate: :rate
      )

    assert length(buckets) == 1
    {_bucket, rate} = List.first(buckets)
    assert rate == 0.0
  end
end
