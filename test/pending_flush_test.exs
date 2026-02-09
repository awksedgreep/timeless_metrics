defmodule PendingFlushTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_pending_flush_test_#{System.os_time(:millisecond)}"

  setup do
    # Use very short flush intervals so tests run fast
    start_supervised!(
      {Timeless,
       name: :pflush_store,
       data_dir: @data_dir,
       buffer_shards: 2,
       flush_interval: 100,
       pending_flush_interval: 200}
    )

    on_exit(fn -> File.rm_rf!(@data_dir) end)
    :ok
  end

  test "data becomes queryable via pending flush without explicit flush call" do
    now = System.os_time(:second)

    # Write points — these land in the current 4-hour segment bucket
    for i <- 0..4 do
      Timeless.write(:pflush_store, "pending_metric", %{"host" => "a"}, 10.0 + i,
        timestamp: now - 300 + i * 60
      )
    end

    # Do NOT call Timeless.flush — wait for the pending flush timer instead
    # Buffer flushes to SegmentBuilder in 5s, pending flush writes to SQLite in 200ms
    # Give it enough time for both stages
    Process.sleep(500)

    {:ok, points} =
      Timeless.query(:pflush_store, "pending_metric", %{"host" => "a"},
        from: now - 600,
        to: now + 60
      )

    assert length(points) == 5,
           "Expected 5 points via pending flush, got #{length(points)}"
  end

  test "pending flush preserves data for continued accumulation" do
    now = System.os_time(:second)

    # Write first batch
    for i <- 0..2 do
      Timeless.write(:pflush_store, "accum_metric", %{"host" => "b"}, i * 1.0,
        timestamp: now - 200 + i * 60
      )
    end

    # Wait for pending flush
    Process.sleep(500)

    # Write second batch (same series, same segment bucket)
    for i <- 3..5 do
      Timeless.write(:pflush_store, "accum_metric", %{"host" => "b"}, i * 1.0,
        timestamp: now - 200 + i * 60
      )
    end

    # Wait for another pending flush cycle
    Process.sleep(500)

    {:ok, points} =
      Timeless.query(:pflush_store, "accum_metric", %{"host" => "b"},
        from: now - 600,
        to: now + 600
      )

    assert length(points) == 6,
           "Expected 6 points after two flush cycles, got #{length(points)}"
  end

  test "info reflects points after pending flush" do
    now = System.os_time(:second)

    for i <- 1..3 do
      Timeless.write(:pflush_store, "info_metric_#{i}", %{"id" => "1"}, i * 1.0,
        timestamp: now
      )
    end

    # Wait for pending flush
    Process.sleep(500)

    info = Timeless.info(:pflush_store)
    assert info.total_points >= 3,
           "Expected at least 3 points in info, got #{info.total_points}"
    assert info.series_count >= 3
  end
end
