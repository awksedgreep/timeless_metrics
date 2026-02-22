defmodule TimelessMetrics.Actor.EngineTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/actor_engine_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {TimelessMetrics,
       name: :actor_test,
       data_dir: @data_dir,
       engine: :actor,
       block_size: 1000,
       max_blocks: 100,
       compression: :zstd}
    )

    on_exit(fn -> File.rm_rf!(@data_dir) end)
    :ok
  end

  test "write and query raw points" do
    now = System.os_time(:second)

    for i <- 0..9 do
      TimelessMetrics.write(:actor_test, "cpu_usage", %{"host" => "web-1"}, 50.0 + i,
        timestamp: now - 3600 + i * 60
      )
    end

    # Give casts time to process
    Process.sleep(50)

    {:ok, points} =
      TimelessMetrics.query(:actor_test, "cpu_usage", %{"host" => "web-1"},
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

    TimelessMetrics.write_batch(:actor_test, entries)
    Process.sleep(50)

    {:ok, points} =
      TimelessMetrics.query(:actor_test, "memory_used", %{"host" => "db-1"},
        from: now - 600,
        to: now
      )

    assert length(points) == 5
  end

  test "query_multi with label filter" do
    now = System.os_time(:second)

    TimelessMetrics.write(:actor_test, "cpu", %{"host" => "a", "region" => "us"}, 10.0,
      timestamp: now
    )

    TimelessMetrics.write(:actor_test, "cpu", %{"host" => "b", "region" => "us"}, 20.0,
      timestamp: now
    )

    TimelessMetrics.write(:actor_test, "cpu", %{"host" => "c", "region" => "eu"}, 30.0,
      timestamp: now
    )

    Process.sleep(50)

    {:ok, results} =
      TimelessMetrics.query_multi(:actor_test, "cpu", %{"region" => "us"},
        from: now - 60,
        to: now + 60
      )

    assert length(results) == 2
    hosts = Enum.map(results, fn %{labels: l} -> l["host"] end) |> Enum.sort()
    assert hosts == ["a", "b"]
  end

  test "query_aggregate with hourly buckets" do
    now = System.os_time(:second)
    hour_start = div(now, 3600) * 3600

    for i <- 0..5 do
      TimelessMetrics.write(:actor_test, "temp", %{"sensor" => "a"}, 20.0 + i,
        timestamp: hour_start + i * 60
      )
    end

    Process.sleep(50)

    {:ok, buckets} =
      TimelessMetrics.query_aggregate(:actor_test, "temp", %{"sensor" => "a"},
        from: hour_start - 3600,
        to: hour_start + 3600,
        bucket: :hour,
        aggregate: :avg
      )

    assert length(buckets) >= 1
    {_ts, avg} = List.first(buckets)
    assert_in_delta avg, 22.5, 0.01
  end

  test "query_aggregate_grouped with cross-series aggregation" do
    now = System.os_time(:second)
    min_start = div(now, 60) * 60

    TimelessMetrics.write(:actor_test, "disk", %{"host" => "a", "device" => "sda"}, 80.0,
      timestamp: min_start
    )

    TimelessMetrics.write(:actor_test, "disk", %{"host" => "a", "device" => "sdb"}, 60.0,
      timestamp: min_start
    )

    TimelessMetrics.write(:actor_test, "disk", %{"host" => "b", "device" => "sda"}, 40.0,
      timestamp: min_start
    )

    Process.sleep(50)

    {:ok, grouped} =
      TimelessMetrics.query_aggregate_grouped(:actor_test, "disk", %{},
        from: min_start - 60,
        to: min_start + 60,
        bucket: :minute,
        aggregate: :max,
        group_by: "host",
        cross_series_aggregate: :max
      )

    assert length(grouped) == 2

    by_host =
      grouped
      |> Enum.map(fn %{group: g, data: d} -> {g["host"], d} end)
      |> Map.new()

    # Host "a" has max(80, 60) = 80
    [{_, host_a_val}] = by_host["a"]
    assert host_a_val == 80.0

    # Host "b" has max(40) = 40
    [{_, host_b_val}] = by_host["b"]
    assert host_b_val == 40.0
  end

  test "latest value" do
    now = System.os_time(:second)

    TimelessMetrics.write(:actor_test, "gauge", %{"id" => "1"}, 100.0, timestamp: now - 60)
    TimelessMetrics.write(:actor_test, "gauge", %{"id" => "1"}, 200.0, timestamp: now)
    Process.sleep(50)

    {:ok, {_ts, val}} = TimelessMetrics.latest(:actor_test, "gauge", %{"id" => "1"})
    assert val == 200.0
  end

  test "latest_multi" do
    now = System.os_time(:second)

    TimelessMetrics.write(:actor_test, "cpu_lat", %{"host" => "a"}, 10.0, timestamp: now)
    TimelessMetrics.write(:actor_test, "cpu_lat", %{"host" => "b"}, 20.0, timestamp: now)
    Process.sleep(50)

    {:ok, results} = TimelessMetrics.latest_multi(:actor_test, "cpu_lat", %{})
    assert length(results) == 2
    values = Enum.map(results, & &1.value) |> Enum.sort()
    assert values == [10.0, 20.0]
  end

  test "flush persists data to disk" do
    now = System.os_time(:second)

    TimelessMetrics.write(:actor_test, "flush_test", %{"id" => "1"}, 42.0, timestamp: now)
    Process.sleep(50)

    # Flush should not raise
    TimelessMetrics.flush(:actor_test)

    # Data should still be queryable
    {:ok, points} =
      TimelessMetrics.query(:actor_test, "flush_test", %{"id" => "1"},
        from: now - 60,
        to: now + 60
      )

    assert [{^now, 42.0}] = points
  end

  test "separate series by labels" do
    now = System.os_time(:second)

    TimelessMetrics.write(:actor_test, "cpu_sep", %{"host" => "a"}, 10.0, timestamp: now)
    TimelessMetrics.write(:actor_test, "cpu_sep", %{"host" => "b"}, 90.0, timestamp: now)
    Process.sleep(50)

    {:ok, points_a} =
      TimelessMetrics.query(:actor_test, "cpu_sep", %{"host" => "a"},
        from: now - 60,
        to: now + 60
      )

    {:ok, points_b} =
      TimelessMetrics.query(:actor_test, "cpu_sep", %{"host" => "b"},
        from: now - 60,
        to: now + 60
      )

    assert [{_, 10.0}] = points_a
    assert [{_, 90.0}] = points_b
  end

  test "list_metrics" do
    now = System.os_time(:second)

    TimelessMetrics.write(:actor_test, "metric_a", %{"id" => "1"}, 1.0, timestamp: now)
    TimelessMetrics.write(:actor_test, "metric_b", %{"id" => "1"}, 2.0, timestamp: now)
    Process.sleep(50)

    {:ok, metrics} = TimelessMetrics.list_metrics(:actor_test)
    assert "metric_a" in metrics
    assert "metric_b" in metrics
  end

  test "list_series" do
    now = System.os_time(:second)

    TimelessMetrics.write(:actor_test, "cpu_ls", %{"host" => "a"}, 1.0, timestamp: now)
    TimelessMetrics.write(:actor_test, "cpu_ls", %{"host" => "b"}, 2.0, timestamp: now)
    Process.sleep(50)

    {:ok, series} = TimelessMetrics.list_series(:actor_test, "cpu_ls")
    labels = Enum.map(series, & &1.labels) |> Enum.sort_by(& &1["host"])
    assert [%{"host" => "a"}, %{"host" => "b"}] = labels
  end

  test "label_values" do
    now = System.os_time(:second)

    TimelessMetrics.write(:actor_test, "cpu_lv", %{"host" => "web-1"}, 1.0, timestamp: now)
    TimelessMetrics.write(:actor_test, "cpu_lv", %{"host" => "web-2"}, 2.0, timestamp: now)
    TimelessMetrics.write(:actor_test, "cpu_lv", %{"host" => "web-3"}, 3.0, timestamp: now)
    Process.sleep(50)

    {:ok, values} = TimelessMetrics.label_values(:actor_test, "cpu_lv", "host")
    assert values == ["web-1", "web-2", "web-3"]
  end

  test "regex label matching" do
    now = System.os_time(:second)

    TimelessMetrics.write(:actor_test, "cpu_re", %{"host" => "web-1"}, 10.0, timestamp: now)
    TimelessMetrics.write(:actor_test, "cpu_re", %{"host" => "web-2"}, 20.0, timestamp: now)
    TimelessMetrics.write(:actor_test, "cpu_re", %{"host" => "db-1"}, 30.0, timestamp: now)
    Process.sleep(50)

    {:ok, results} =
      TimelessMetrics.query_multi(:actor_test, "cpu_re", %{"host" => {:regex, "web-.*"}},
        from: now - 60,
        to: now + 60
      )

    assert length(results) == 2
    hosts = Enum.map(results, fn %{labels: l} -> l["host"] end) |> Enum.sort()
    assert hosts == ["web-1", "web-2"]
  end

  test "query_aggregate_multi_filtered with threshold" do
    now = System.os_time(:second)
    min_start = div(now, 60) * 60

    TimelessMetrics.write(:actor_test, "cpu_filt", %{"host" => "a"}, 90.0,
      timestamp: min_start
    )

    TimelessMetrics.write(:actor_test, "cpu_filt", %{"host" => "b"}, 30.0,
      timestamp: min_start
    )

    Process.sleep(50)

    {:ok, results} =
      TimelessMetrics.query_aggregate_multi_filtered(:actor_test, "cpu_filt", %{},
        from: min_start - 60,
        to: min_start + 60,
        bucket: :minute,
        aggregate: :avg,
        threshold: {:gt, 50.0}
      )

    assert length(results) == 1
    assert List.first(results).labels["host"] == "a"
  end

  test "query_aggregate_multi_metrics" do
    now = System.os_time(:second)
    min_start = div(now, 60) * 60

    TimelessMetrics.write(:actor_test, "metric_x", %{"id" => "1"}, 10.0,
      timestamp: min_start
    )

    TimelessMetrics.write(:actor_test, "metric_y", %{"id" => "1"}, 20.0,
      timestamp: min_start
    )

    Process.sleep(50)

    {:ok, results} =
      TimelessMetrics.query_aggregate_multi_metrics(
        :actor_test,
        ["metric_x", "metric_y"],
        %{},
        from: min_start - 60,
        to: min_start + 60,
        bucket: :minute,
        aggregate: :avg
      )

    assert length(results) == 2
    metrics = Enum.map(results, & &1.metric) |> Enum.sort()
    assert metrics == ["metric_x", "metric_y"]
  end

  test "compression round-trip preserves data" do
    now = System.os_time(:second)

    expected =
      for i <- 0..49 do
        ts = now - 5000 + i * 10
        val = :math.sin(i / 10) * 50 + 50
        TimelessMetrics.write(:actor_test, "sine", %{"ch" => "0"}, val, timestamp: ts)
        {ts, val}
      end

    Process.sleep(50)

    {:ok, actual} =
      TimelessMetrics.query(:actor_test, "sine", %{"ch" => "0"}, from: now - 6000, to: now)

    assert length(actual) == 50

    Enum.zip(expected, actual)
    |> Enum.each(fn {{exp_ts, exp_val}, {act_ts, act_val}} ->
      assert exp_ts == act_ts
      assert_in_delta exp_val, act_val, 0.001
    end)
  end

  test "persistent_term stores data_dir" do
    assert :persistent_term.get({TimelessMetrics, :actor_test, :data_dir}) == @data_dir
  end
end
