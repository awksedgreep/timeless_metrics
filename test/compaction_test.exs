defmodule TimelessMetrics.CompactionTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_compaction_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {TimelessMetrics,
       name: :compact_test,
       data_dir: @data_dir,
       buffer_shards: 1,
       segment_duration: 3_600}
    )

    on_exit(fn ->
      :persistent_term.erase({TimelessMetrics, :compact_test, :schema})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "tier_dead_bytes returns 0 when no dead space" do
    now = System.os_time(:second)
    hour_start = div(now - 7200, 3600) * 3600

    for i <- 0..5 do
      TimelessMetrics.write(:compact_test, "cpu", %{"host" => "a"}, 10.0 + i,
        timestamp: hour_start + i * 300
      )
    end

    TimelessMetrics.flush(:compact_test)
    TimelessMetrics.rollup(:compact_test)

    builder = :compact_test_builder_0
    {dead, total} = TimelessMetrics.SegmentBuilder.tier_dead_bytes(builder, :hourly)

    assert total > 0
    assert dead == 0
  end

  test "catch_up creates dead space from read-modify-write of existing chunks" do
    now = System.os_time(:second)
    hour_start = div(now - 7200, 3600) * 3600

    # Write points and rollup — this creates the initial chunk
    for i <- 0..11 do
      TimelessMetrics.write(:compact_test, "cpu", %{"host" => "a"}, 10.0 + i,
        timestamp: hour_start + i * 300
      )
    end

    TimelessMetrics.flush(:compact_test)
    TimelessMetrics.rollup(:compact_test)

    builder = :compact_test_builder_0
    {dead_before, total_before} = TimelessMetrics.SegmentBuilder.tier_dead_bytes(builder, :hourly)
    assert dead_before == 0
    assert total_before > 0

    # catch_up re-reads [watermark - lookback, watermark) and merges into
    # existing chunks, creating dead space (new version appended, old stays)
    TimelessMetrics.catch_up(:compact_test)

    {dead_after, total_after} = TimelessMetrics.SegmentBuilder.tier_dead_bytes(builder, :hourly)

    # File grew with the new version; old version is dead space
    assert total_after > total_before
    assert dead_after > 0
  end

  test "compact_tier reclaims dead space" do
    now = System.os_time(:second)
    hour_start = div(now - 7200, 3600) * 3600

    # Create initial data and rollup
    for i <- 0..11 do
      TimelessMetrics.write(:compact_test, "cpu", %{"host" => "a"}, 10.0 + i,
        timestamp: hour_start + i * 300
      )
    end

    TimelessMetrics.flush(:compact_test)
    TimelessMetrics.rollup(:compact_test)

    # catch_up creates dead space
    TimelessMetrics.catch_up(:compact_test)

    builder = :compact_test_builder_0
    {dead, _total} = TimelessMetrics.SegmentBuilder.tier_dead_bytes(builder, :hourly)
    assert dead > 0

    # Force compaction with low threshold
    {:ok, reclaimed} = TimelessMetrics.SegmentBuilder.compact_tier(builder, :hourly, threshold: 0.0)
    assert reclaimed == dead

    # After compaction, dead space should be 0
    {dead_after, _total_after} = TimelessMetrics.SegmentBuilder.tier_dead_bytes(builder, :hourly)
    assert dead_after == 0
  end

  test "data survives compaction and queries return correct results" do
    now = System.os_time(:second)
    hour_start = div(now - 7200, 3600) * 3600

    # Write 12 points and rollup, then catch_up to create dead space
    for i <- 0..11 do
      TimelessMetrics.write(:compact_test, "cpu", %{"host" => "a"}, 10.0 + i,
        timestamp: hour_start + i * 300
      )
    end

    TimelessMetrics.flush(:compact_test)
    TimelessMetrics.rollup(:compact_test)
    TimelessMetrics.catch_up(:compact_test)

    # Query before compaction
    {:ok, rows_before} =
      TimelessMetrics.query_tier(:compact_test, :hourly, "cpu", %{"host" => "a"},
        from: hour_start - 3600,
        to: hour_start + 7200
      )

    assert length(rows_before) >= 1
    row_before = List.first(rows_before)

    # Compact
    builder = :compact_test_builder_0
    {:ok, _reclaimed} = TimelessMetrics.SegmentBuilder.compact_tier(builder, :hourly, threshold: 0.0)

    # Query after compaction — same results
    {:ok, rows_after} =
      TimelessMetrics.query_tier(:compact_test, :hourly, "cpu", %{"host" => "a"},
        from: hour_start - 3600,
        to: hour_start + 7200
      )

    assert length(rows_after) == length(rows_before)
    row_after = List.first(rows_after)

    assert row_after.avg == row_before.avg
    assert row_after.min == row_before.min
    assert row_after.max == row_before.max
    assert row_after.count == row_before.count
    assert row_after.sum == row_before.sum
  end

  test "compact_tier returns :noop when below threshold" do
    now = System.os_time(:second)
    hour_start = div(now - 7200, 3600) * 3600

    for i <- 0..5 do
      TimelessMetrics.write(:compact_test, "cpu", %{"host" => "a"}, 10.0 + i,
        timestamp: hour_start + i * 300
      )
    end

    TimelessMetrics.flush(:compact_test)
    TimelessMetrics.rollup(:compact_test)

    builder = :compact_test_builder_0
    result = TimelessMetrics.SegmentBuilder.compact_tier(builder, :hourly)
    assert result == :noop
  end

  test "compact_tier returns :noop on empty tier" do
    builder = :compact_test_builder_0
    result = TimelessMetrics.SegmentBuilder.compact_tier(builder, :hourly)
    assert result == :noop
  end

  test "TimelessMetrics.compact/2 compacts all tiers across all shards" do
    now = System.os_time(:second)
    hour_start = div(now - 7200, 3600) * 3600

    # Create data, rollup, then catch_up to create dead space
    for i <- 0..11 do
      TimelessMetrics.write(:compact_test, "cpu", %{"host" => "a"}, 10.0 + i,
        timestamp: hour_start + i * 300
      )
    end

    TimelessMetrics.flush(:compact_test)
    TimelessMetrics.rollup(:compact_test)
    TimelessMetrics.catch_up(:compact_test)

    # Compact with threshold 0 to force it
    result = TimelessMetrics.compact(:compact_test, threshold: 0.0)

    assert is_map(result)
    assert Map.has_key?(result, :hourly)
    assert result[:hourly] > 0
  end

  test "info includes dead_bytes in tier stats" do
    now = System.os_time(:second)
    hour_start = div(now - 7200, 3600) * 3600

    for i <- 0..5 do
      TimelessMetrics.write(:compact_test, "cpu", %{"host" => "a"}, 10.0 + i,
        timestamp: hour_start + i * 300
      )
    end

    TimelessMetrics.flush(:compact_test)
    TimelessMetrics.rollup(:compact_test)

    info = TimelessMetrics.info(:compact_test)
    assert info.tiers[:hourly].dead_bytes == 0

    # catch_up creates dead space
    TimelessMetrics.catch_up(:compact_test)

    info = TimelessMetrics.info(:compact_test)
    assert info.tiers[:hourly].dead_bytes > 0
  end
end
