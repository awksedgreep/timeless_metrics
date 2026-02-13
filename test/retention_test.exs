defmodule TimelessMetrics.RetentionTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_retention_test_#{System.os_time(:millisecond)}"

  setup do
    # Use very short retention for testing
    schema = %TimelessMetrics.Schema{
      raw_retention_seconds: 3_600,  # 1 hour
      rollup_interval: :timer.hours(1),  # won't auto-fire in tests
      retention_interval: :timer.hours(1),
      tiers: [
        %TimelessMetrics.Schema.Tier{
          name: :hourly,
          resolution_seconds: 3_600,
          aggregates: [:avg, :min, :max, :count, :sum, :last],
          retention_seconds: 7_200,  # 2 hours
          table_name: "tier_hourly"
        }
      ]
    }

    start_supervised!(
      {TimelessMetrics,
       name: :ret_test,
       data_dir: @data_dir,
       buffer_shards: 1,
       schema: schema}
    )

    on_exit(fn ->
      :persistent_term.erase({TimelessMetrics, :ret_test, :schema})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "retention deletes expired raw segments" do
    now = System.os_time(:second)

    # Write old data (2 hours ago — outside 1-hour raw retention)
    for i <- 0..5 do
      TimelessMetrics.write(:ret_test, "old_metric", %{"id" => "1"}, 42.0,
        timestamp: now - 7200 + i * 60
      )
    end

    # Write recent data (5 minutes ago — within retention)
    for i <- 0..5 do
      TimelessMetrics.write(:ret_test, "new_metric", %{"id" => "1"}, 99.0,
        timestamp: now - 300 + i * 10
      )
    end

    TimelessMetrics.flush(:ret_test)

    # Verify both exist before retention
    info_before = TimelessMetrics.info(:ret_test)
    assert info_before.series_count == 2

    # Enforce retention
    TimelessMetrics.enforce_retention(:ret_test)

    # Old data should be gone, new data should remain
    {:ok, old_points} =
      TimelessMetrics.query(:ret_test, "old_metric", %{"id" => "1"},
        from: now - 10_000,
        to: now
      )

    {:ok, new_points} =
      TimelessMetrics.query(:ret_test, "new_metric", %{"id" => "1"},
        from: now - 10_000,
        to: now
      )

    assert old_points == []
    assert length(new_points) == 6
  end

  test "retention cleans orphaned series" do
    now = System.os_time(:second)

    # Write only old data for this series
    TimelessMetrics.write(:ret_test, "orphan", %{"id" => "1"}, 1.0,
      timestamp: now - 7200
    )

    TimelessMetrics.flush(:ret_test)
    TimelessMetrics.enforce_retention(:ret_test)

    info = TimelessMetrics.info(:ret_test)
    assert info.series_count == 0
  end

  test "retention cleans orphaned alert rules" do
    now = System.os_time(:second)

    # Create a metric with data so its series exists
    TimelessMetrics.write(:ret_test, "active_metric", %{"host" => "a"}, 42.0,
      timestamp: now - 300
    )

    TimelessMetrics.flush(:ret_test)

    # Create alert for active metric — should survive
    {:ok, active_id} =
      TimelessMetrics.create_alert(:ret_test,
        name: "Active alert",
        metric: "active_metric",
        condition: :above,
        threshold: 90.0
      )

    # Create alert for a metric that doesn't exist — orphaned
    {:ok, orphan_id} =
      TimelessMetrics.create_alert(:ret_test,
        name: "Orphaned alert",
        metric: "nonexistent_metric",
        condition: :above,
        threshold: 50.0
      )

    {:ok, rules_before} = TimelessMetrics.list_alerts(:ret_test)
    assert length(rules_before) == 2

    # Enforce retention — should clean up orphaned rule
    TimelessMetrics.enforce_retention(:ret_test)

    {:ok, rules_after} = TimelessMetrics.list_alerts(:ret_test)
    assert length(rules_after) == 1
    assert List.first(rules_after).id == active_id
    assert Enum.find(rules_after, &(&1.id == orphan_id)) == nil
  end
end
