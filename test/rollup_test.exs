defmodule TimelessMetrics.RollupTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_rollup_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {TimelessMetrics,
       name: :rollup_test, data_dir: @data_dir, buffer_shards: 1, segment_duration: 3_600}
    )

    on_exit(fn ->
      :persistent_term.erase({TimelessMetrics, :rollup_test, :schema})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "hourly rollup computes correct aggregates" do
    now = System.os_time(:second)
    # Use a past hour boundary so the segment is "complete"
    hour_start = div(now - 7200, 3600) * 3600

    # Write 12 points (every 5 minutes) in a past hour
    for i <- 0..11 do
      TimelessMetrics.write(:rollup_test, "cpu", %{"host" => "a"}, 10.0 + i,
        timestamp: hour_start + i * 300
      )
    end

    TimelessMetrics.flush(:rollup_test)

    # Run rollup
    TimelessMetrics.rollup(:rollup_test)

    # Read hourly tier
    {:ok, rows} =
      TimelessMetrics.query_tier(:rollup_test, :hourly, "cpu", %{"host" => "a"},
        from: hour_start - 3600,
        to: hour_start + 7200
      )

    assert length(rows) >= 1
    row = List.first(rows)

    # 10, 11, 12, ..., 21 -> avg=15.5, min=10, max=21, count=12, sum=186
    assert row.count == 12
    assert_in_delta row.avg, 15.5, 0.01
    assert_in_delta row.min, 10.0, 0.01
    assert_in_delta row.max, 21.0, 0.01
    assert_in_delta row.sum, 186.0, 0.01
    assert_in_delta row.last, 21.0, 0.01
  end

  test "daily rollup cascades from hourly" do
    now = System.os_time(:second)
    # Write data across multiple past hours in a past day
    day_start = div(now - 172_800, 86_400) * 86_400

    for hour <- 0..3 do
      hour_start = day_start + hour * 3600

      for i <- 0..5 do
        TimelessMetrics.write(:rollup_test, "temp", %{"sensor" => "1"}, 20.0 + hour + i * 0.1,
          timestamp: hour_start + i * 300
        )
      end
    end

    TimelessMetrics.flush(:rollup_test)

    # Run hourly rollup first, then daily
    TimelessMetrics.rollup(:rollup_test, :hourly)
    TimelessMetrics.rollup(:rollup_test, :daily)

    # Read daily tier
    {:ok, daily_rows} =
      TimelessMetrics.query_tier(:rollup_test, :daily, "temp", %{"sensor" => "1"},
        from: day_start - 86_400,
        to: day_start + 172_800
      )

    assert length(daily_rows) >= 1
    row = List.first(daily_rows)

    # 4 hours × 6 points = 24 total points
    assert row.count == 24
    assert row.min != nil
    assert row.max != nil
    assert row.avg != nil
  end

  test "rollup is idempotent" do
    now = System.os_time(:second)
    hour_start = div(now - 7200, 3600) * 3600

    for i <- 0..5 do
      TimelessMetrics.write(:rollup_test, "gauge", %{"id" => "1"}, 50.0 + i,
        timestamp: hour_start + i * 300
      )
    end

    TimelessMetrics.flush(:rollup_test)

    # Run rollup twice
    TimelessMetrics.rollup(:rollup_test, :hourly)
    TimelessMetrics.rollup(:rollup_test, :hourly)

    {:ok, rows} =
      TimelessMetrics.query_tier(:rollup_test, :hourly, "gauge", %{"id" => "1"},
        from: hour_start - 3600,
        to: hour_start + 7200
      )

    # Should still be exactly one row, not duplicated
    assert length(rows) == 1
  end

  test "query_aggregate reads from rollup tier when available" do
    now = System.os_time(:second)
    hour_start = div(now - 7200, 3600) * 3600

    for i <- 0..11 do
      TimelessMetrics.write(:rollup_test, "net", %{"if" => "eth0"}, 100.0 + i * 10,
        timestamp: hour_start + i * 300
      )
    end

    TimelessMetrics.flush(:rollup_test)
    TimelessMetrics.rollup(:rollup_test, :hourly)

    # Query with hourly bucket — should use the rollup tier
    {:ok, buckets} =
      TimelessMetrics.query_aggregate(:rollup_test, "net", %{"if" => "eth0"},
        from: hour_start - 3600,
        to: hour_start + 7200,
        bucket: :hour,
        aggregate: :avg
      )

    assert length(buckets) >= 1
  end

  test "late arrivals get caught up into rollup tiers" do
    now = System.os_time(:second)
    hour_start = div(now - 7200, 3600) * 3600

    # Write initial data and rollup
    for i <- 0..5 do
      TimelessMetrics.write(:rollup_test, "late_metric", %{"id" => "1"}, 10.0 + i,
        timestamp: hour_start + i * 300
      )
    end

    TimelessMetrics.flush(:rollup_test)
    TimelessMetrics.rollup(:rollup_test, :hourly)

    # Verify initial rollup
    {:ok, [initial_row]} =
      TimelessMetrics.query_tier(:rollup_test, :hourly, "late_metric", %{"id" => "1"},
        from: hour_start - 3600,
        to: hour_start + 7200
      )

    assert initial_row.count == 6
    assert_in_delta initial_row.avg, 12.5, 0.01

    # Now a "late" point arrives for the already-rolled-up hour
    TimelessMetrics.write(:rollup_test, "late_metric", %{"id" => "1"}, 100.0,
      timestamp: hour_start + 1800
    )

    TimelessMetrics.flush(:rollup_test)

    # Normal rollup won't pick it up (watermark already advanced)
    TimelessMetrics.rollup(:rollup_test, :hourly)

    {:ok, [still_old]} =
      TimelessMetrics.query_tier(:rollup_test, :hourly, "late_metric", %{"id" => "1"},
        from: hour_start - 3600,
        to: hour_start + 7200
      )

    # Count still 6 — the late point was missed
    assert still_old.count == 6

    # Run the late-arrival catch-up
    TimelessMetrics.catch_up(:rollup_test)

    {:ok, [updated_row]} =
      TimelessMetrics.query_tier(:rollup_test, :hourly, "late_metric", %{"id" => "1"},
        from: hour_start - 3600,
        to: hour_start + 7200
      )

    # Now count is 7 and the average has shifted to include the 100.0
    assert updated_row.count == 7
    expected_avg = (10.0 + 11.0 + 12.0 + 13.0 + 14.0 + 15.0 + 100.0) / 7
    assert_in_delta updated_row.avg, expected_avg, 0.01
    assert_in_delta updated_row.max, 100.0, 0.01
  end

  test "query stitches rollup + raw across watermark boundary" do
    now = System.os_time(:second)

    # --- Old data: 3 complete past hours (will be rolled up) ---
    # 3 hours ago
    old_hour_1 = div(now - 10800, 3600) * 3600
    # 2 hours ago
    old_hour_2 = old_hour_1 + 3600
    # 1 hour ago
    old_hour_3 = old_hour_2 + 3600

    for {hour_start, base_val} <- [{old_hour_1, 10.0}, {old_hour_2, 20.0}, {old_hour_3, 30.0}] do
      for i <- 0..5 do
        TimelessMetrics.write(:rollup_test, "stitch", %{"id" => "1"}, base_val + i,
          timestamp: hour_start + i * 300
        )
      end
    end

    # --- Recent data: current (incomplete) hour (will NOT be rolled up) ---
    current_hour = div(now, 3600) * 3600

    for i <- 0..2 do
      TimelessMetrics.write(:rollup_test, "stitch", %{"id" => "1"}, 50.0 + i,
        timestamp: current_hour + i * 300
      )
    end

    TimelessMetrics.flush(:rollup_test)
    TimelessMetrics.rollup(:rollup_test, :hourly)

    # Query spanning old (rolled up) + current (raw only) hours
    {:ok, buckets} =
      TimelessMetrics.query_aggregate(:rollup_test, "stitch", %{"id" => "1"},
        from: old_hour_1,
        to: current_hour + 3600,
        bucket: :hour,
        aggregate: :avg
      )

    # Should have data from both rollup (old hours) and raw (current hour)
    # At least 3 old buckets + 1 current bucket
    assert length(buckets) >= 4,
           "Expected stitched results from rollup + raw, got #{length(buckets)} buckets"

    # Verify the old buckets came from rollup (should have correct avg)
    old_bucket = Enum.find(buckets, fn {ts, _} -> ts == old_hour_1 end)
    assert old_bucket != nil, "Missing rolled-up bucket for old_hour_1"
    {_, old_avg} = old_bucket
    # 10, 11, 12, 13, 14, 15 -> avg = 12.5
    assert_in_delta old_avg, 12.5, 0.01

    # Verify the current bucket came from raw
    current_bucket = Enum.find(buckets, fn {ts, _} -> ts == current_hour end)
    assert current_bucket != nil, "Missing raw bucket for current hour"
    {_, current_avg} = current_bucket
    # 50, 51, 52 -> avg = 51.0
    assert_in_delta current_avg, 51.0, 0.01
  end
end
