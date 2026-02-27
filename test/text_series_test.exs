defmodule TimelessMetrics.TextSeriesTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/text_series_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {TimelessMetrics, name: :text_test_store, data_dir: @data_dir, engine: :actor}
    )

    on_exit(fn -> File.rm_rf!(@data_dir) end)
    :ok
  end

  test "write text and query text round-trip" do
    now = System.os_time(:second)

    TimelessMetrics.write_text(:text_test_store, "sysDescr", %{"host" => "router1"},
      "Cisco IOS 15.2",
      timestamp: now
    )

    TimelessMetrics.write_text(:text_test_store, "sysDescr", %{"host" => "router1"},
      "Cisco IOS 15.2",
      timestamp: now + 60
    )

    TimelessMetrics.flush(:text_test_store)

    {:ok, points} =
      TimelessMetrics.query_text(:text_test_store, "sysDescr", %{"host" => "router1"},
        from: now - 60,
        to: now + 120
      )

    assert length(points) == 2
    assert {^now, "Cisco IOS 15.2"} = List.first(points)
  end

  test "text batch write" do
    now = System.os_time(:second)

    entries = [
      {"ifDescr", %{"ifIndex" => "1"}, "GigabitEthernet0/0", now},
      {"ifDescr", %{"ifIndex" => "1"}, "GigabitEthernet0/0", now + 60},
      {"ifDescr", %{"ifIndex" => "2"}, "FastEthernet0/1", now}
    ]

    TimelessMetrics.write_text_batch(:text_test_store, entries)
    TimelessMetrics.flush(:text_test_store)

    {:ok, points1} =
      TimelessMetrics.query_text(:text_test_store, "ifDescr", %{"ifIndex" => "1"},
        from: now - 60,
        to: now + 120
      )

    assert length(points1) == 2

    {:ok, points2} =
      TimelessMetrics.query_text(:text_test_store, "ifDescr", %{"ifIndex" => "2"},
        from: now - 60,
        to: now + 120
      )

    assert length(points2) == 1
    assert {^now, "FastEthernet0/1"} = hd(points2)
  end

  test "text persistence across restart" do
    now = System.os_time(:second)

    TimelessMetrics.write_text(:text_test_store, "firmware", %{"device" => "sw1"},
      "v3.2.1",
      timestamp: now
    )

    TimelessMetrics.flush(:text_test_store)

    # Stop and restart
    stop_supervised!({TimelessMetrics, :text_test_store})

    :persistent_term.erase(
      {TimelessMetrics.Actor.SeriesManager, :text_test_store_actor_manager}
    )

    start_supervised!(
      {TimelessMetrics, name: :text_test_store, data_dir: @data_dir, engine: :actor}
    )

    Process.sleep(500)

    {:ok, points} =
      TimelessMetrics.query_text(:text_test_store, "firmware", %{"device" => "sw1"},
        from: now - 60,
        to: now + 60
      )

    assert length(points) == 1
    assert {^now, "v3.2.1"} = hd(points)
  end

  test "text compression trigger at block_size" do
    # Use a low-level approach to test compression trigger
    test_id = System.unique_integer([:positive])
    registry_name = :"text_comp_registry_#{test_id}"
    sup_name = :"text_comp_sup_#{test_id}"
    db_name = :"text_comp_db_#{test_id}"
    data_dir = "/tmp/text_comp_test_#{test_id}"
    File.mkdir_p!(data_dir)

    start_supervised!({Registry, keys: :unique, name: registry_name})
    start_supervised!({DynamicSupervisor, name: sup_name, strategy: :one_for_one})
    start_supervised!({TimelessMetrics.DB, name: db_name, data_dir: data_dir})

    on_exit(fn -> File.rm_rf!(data_dir) end)

    child_spec = %{
      id: {:series, 1},
      start:
        {TimelessMetrics.Actor.SeriesServer, :start_link,
         [
           [
             series_id: 1,
             metric_name: "text_metric",
             labels: %{},
             store: :"text_comp_store_#{test_id}",
             data_dir: data_dir,
             registry: registry_name,
             block_size: 5,
             max_blocks: 100,
             compression: :zstd,
             flush_interval: 600_000,
             series_type: :text
           ]
         ]},
      restart: :transient
    }

    {:ok, pid} = DynamicSupervisor.start_child(sup_name, child_spec)

    now = System.os_time(:second)

    # Write 5 points to trigger compression
    for i <- 0..4 do
      GenServer.cast(pid, {:write_text, now + i, "value_#{i}"})
    end

    Process.sleep(50)

    state = GenServer.call(pid, :state)
    assert state.block_count == 1
    assert state.raw_count == 0
    assert state.series_type == :text

    # Verify data survived compression
    {:ok, points} = GenServer.call(pid, {:query_raw, now, now + 4})
    assert length(points) == 5
  end

  test "latest_text" do
    now = System.os_time(:second)

    TimelessMetrics.write_text(:text_test_store, "status", %{"id" => "1"},
      "initializing",
      timestamp: now
    )

    TimelessMetrics.write_text(:text_test_store, "status", %{"id" => "1"},
      "running",
      timestamp: now + 60
    )

    TimelessMetrics.flush(:text_test_store)

    {:ok, {ts, val}} = TimelessMetrics.latest_text(:text_test_store, "status", %{"id" => "1"})
    assert ts == now + 60
    assert val == "running"
  end

  test "query_text_multi with label filter" do
    now = System.os_time(:second)

    TimelessMetrics.write_text(:text_test_store, "ifName", %{"host" => "sw1", "ifIndex" => "1"},
      "Gi0/0",
      timestamp: now
    )

    TimelessMetrics.write_text(:text_test_store, "ifName", %{"host" => "sw1", "ifIndex" => "2"},
      "Gi0/1",
      timestamp: now
    )

    TimelessMetrics.write_text(:text_test_store, "ifName", %{"host" => "sw2", "ifIndex" => "1"},
      "Fa0/0",
      timestamp: now
    )

    TimelessMetrics.flush(:text_test_store)

    {:ok, results} =
      TimelessMetrics.query_text_multi(:text_test_store, "ifName", %{"host" => "sw1"},
        from: now - 60,
        to: now + 60
      )

    assert length(results) == 2

    values = Enum.flat_map(results, fn %{points: pts} -> Enum.map(pts, &elem(&1, 1)) end)
    assert "Gi0/0" in values
    assert "Gi0/1" in values
  end

  test "mixed store: numeric and text series coexist" do
    now = System.os_time(:second)

    # Write numeric
    TimelessMetrics.write(:text_test_store, "cpu_usage", %{"host" => "web1"}, 73.2,
      timestamp: now
    )

    # Write text
    TimelessMetrics.write_text(:text_test_store, "sysDescr", %{"host" => "web1"},
      "Linux 5.15",
      timestamp: now
    )

    TimelessMetrics.flush(:text_test_store)

    # Query numeric
    {:ok, num_points} =
      TimelessMetrics.query(:text_test_store, "cpu_usage", %{"host" => "web1"},
        from: now - 60,
        to: now + 60
      )

    assert [{^now, 73.2}] = num_points

    # Query text
    {:ok, text_points} =
      TimelessMetrics.query_text(:text_test_store, "sysDescr", %{"host" => "web1"},
        from: now - 60,
        to: now + 60
      )

    assert [{^now, "Linux 5.15"}] = text_points
  end

  test "text series skipped in daily rollup" do
    now = System.os_time(:second)
    # Write to a text series
    TimelessMetrics.write_text(:text_test_store, "text_metric", %{"id" => "1"},
      "hello",
      timestamp: now
    )

    TimelessMetrics.flush(:text_test_store)

    # Rollup should not crash — text series return nil from compute_daily
    assert :ok = TimelessMetrics.rollup(:text_test_store)
  end
end
