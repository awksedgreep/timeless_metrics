defmodule TimelessMetricsTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!({TimelessMetrics, name: :test_store, data_dir: @data_dir, buffer_shards: 2})
    on_exit(fn -> File.rm_rf!(@data_dir) end)
    :ok
  end

  test "write and query raw points" do
    now = System.os_time(:second)

    # Write 10 points spread across time
    for i <- 0..9 do
      TimelessMetrics.write(:test_store, "cpu_usage", %{"host" => "web-1"}, 50.0 + i,
        timestamp: now - 3600 + i * 60
      )
    end

    # Flush to disk
    TimelessMetrics.flush(:test_store)

    # Query them back
    {:ok, points} =
      TimelessMetrics.query(:test_store, "cpu_usage", %{"host" => "web-1"},
        from: now - 7200,
        to: now
      )

    assert length(points) == 10
    assert {_, 50.0} = List.first(points)
    assert {_, 59.0} = List.last(points)
  end

  test "write_batch and query" do
    now = System.os_time(:second)

    entries =
      for i <- 0..4 do
        {"memory_used", %{"host" => "db-1"}, 1000.0 + i * 100, now - 300 + i * 60}
      end

    TimelessMetrics.write_batch(:test_store, entries)
    TimelessMetrics.flush(:test_store)

    {:ok, points} =
      TimelessMetrics.query(:test_store, "memory_used", %{"host" => "db-1"},
        from: now - 600,
        to: now
      )

    assert length(points) == 5
  end

  test "query_aggregate with hourly buckets" do
    now = System.os_time(:second)
    # Align to hour boundary for predictable bucketing
    hour_start = div(now, 3600) * 3600

    for i <- 0..5 do
      TimelessMetrics.write(:test_store, "temp", %{"sensor" => "a"}, 20.0 + i,
        timestamp: hour_start + i * 60
      )
    end

    TimelessMetrics.flush(:test_store)

    {:ok, buckets} =
      TimelessMetrics.query_aggregate(:test_store, "temp", %{"sensor" => "a"},
        from: hour_start - 3600,
        to: hour_start + 3600,
        bucket: :hour,
        aggregate: :avg
      )

    assert length(buckets) >= 1
    # Average of 20, 21, 22, 23, 24, 25 = 22.5
    {_bucket_ts, avg} = List.first(buckets)
    assert_in_delta avg, 22.5, 0.01
  end

  test "latest value" do
    now = System.os_time(:second)

    TimelessMetrics.write(:test_store, "gauge", %{"id" => "1"}, 100.0, timestamp: now - 60)
    TimelessMetrics.write(:test_store, "gauge", %{"id" => "1"}, 200.0, timestamp: now)
    TimelessMetrics.flush(:test_store)

    {:ok, {_ts, val}} = TimelessMetrics.latest(:test_store, "gauge", %{"id" => "1"})
    assert val == 200.0
  end

  test "separate series by labels" do
    now = System.os_time(:second)

    TimelessMetrics.write(:test_store, "cpu", %{"host" => "a"}, 10.0, timestamp: now)
    TimelessMetrics.write(:test_store, "cpu", %{"host" => "b"}, 90.0, timestamp: now)
    TimelessMetrics.flush(:test_store)

    {:ok, points_a} =
      TimelessMetrics.query(:test_store, "cpu", %{"host" => "a"}, from: now - 60, to: now + 60)

    {:ok, points_b} =
      TimelessMetrics.query(:test_store, "cpu", %{"host" => "b"}, from: now - 60, to: now + 60)

    assert [{_, 10.0}] = points_a
    assert [{_, 90.0}] = points_b
  end

  test "info returns stats" do
    now = System.os_time(:second)

    for i <- 1..5 do
      TimelessMetrics.write(:test_store, "metric_#{i}", %{"id" => "1"}, i * 1.0, timestamp: now)
    end

    TimelessMetrics.flush(:test_store)

    info = TimelessMetrics.info(:test_store)
    assert info.series_count == 5
    assert info.total_points == 5
    assert info.segment_count >= 1
    assert info.storage_bytes > 0
    assert is_binary(info.db_path)
  end

  test "buffer flushes data on shutdown" do
    now = System.os_time(:second)

    # Write points but do NOT call flush — data sits in the ETS buffer
    for i <- 0..4 do
      TimelessMetrics.write(:test_store, "shutdown_test", %{"id" => "1"}, i * 10.0,
        timestamp: now + i
      )
    end

    # Stop the store (triggers terminate callbacks)
    stop_supervised!({TimelessMetrics, :test_store})

    # Restart the store with the same data_dir
    start_supervised!({TimelessMetrics, name: :test_store, data_dir: @data_dir, buffer_shards: 2})

    # The builder should have received the flush from terminate
    # Give the segment builder a moment to write to SQLite
    Process.sleep(200)
    TimelessMetrics.flush(:test_store)

    {:ok, points} =
      TimelessMetrics.query(:test_store, "shutdown_test", %{"id" => "1"},
        from: now - 60,
        to: now + 60
      )

    assert length(points) == 5, "Expected 5 points to survive shutdown, got #{length(points)}"
  end

  test "compression round-trip preserves data" do
    now = System.os_time(:second)

    # Write many points with varied values
    expected =
      for i <- 0..99 do
        ts = now - 5000 + i * 10
        val = :math.sin(i / 10) * 50 + 50
        TimelessMetrics.write(:test_store, "sine", %{"ch" => "0"}, val, timestamp: ts)
        {ts, val}
      end

    TimelessMetrics.flush(:test_store)

    {:ok, actual} =
      TimelessMetrics.query(:test_store, "sine", %{"ch" => "0"}, from: now - 6000, to: now)

    assert length(actual) == 100

    Enum.zip(expected, actual)
    |> Enum.each(fn {{exp_ts, exp_val}, {act_ts, act_val}} ->
      assert exp_ts == act_ts
      assert_in_delta exp_val, act_val, 0.001
    end)
  end
end
