defmodule Timeless.RetentionTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_retention_test_#{System.os_time(:millisecond)}"

  setup do
    # Use very short retention for testing
    schema = %Timeless.Schema{
      raw_retention_seconds: 3_600,  # 1 hour
      rollup_interval: :timer.hours(1),  # won't auto-fire in tests
      retention_interval: :timer.hours(1),
      tiers: [
        %Timeless.Schema.Tier{
          name: :hourly,
          resolution_seconds: 3_600,
          aggregates: [:avg, :min, :max, :count, :sum, :last],
          retention_seconds: 7_200,  # 2 hours
          table_name: "tier_hourly"
        }
      ]
    }

    start_supervised!(
      {Timeless,
       name: :ret_test,
       data_dir: @data_dir,
       buffer_shards: 1,
       schema: schema}
    )

    on_exit(fn ->
      :persistent_term.erase({Timeless, :ret_test, :schema})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "retention deletes expired raw segments" do
    now = System.os_time(:second)

    # Write old data (2 hours ago — outside 1-hour raw retention)
    for i <- 0..5 do
      Timeless.write(:ret_test, "old_metric", %{"id" => "1"}, 42.0,
        timestamp: now - 7200 + i * 60
      )
    end

    # Write recent data (5 minutes ago — within retention)
    for i <- 0..5 do
      Timeless.write(:ret_test, "new_metric", %{"id" => "1"}, 99.0,
        timestamp: now - 300 + i * 10
      )
    end

    Timeless.flush(:ret_test)

    # Verify both exist before retention
    info_before = Timeless.info(:ret_test)
    assert info_before.series_count == 2

    # Enforce retention
    Timeless.enforce_retention(:ret_test)

    # Old data should be gone, new data should remain
    {:ok, old_points} =
      Timeless.query(:ret_test, "old_metric", %{"id" => "1"},
        from: now - 10_000,
        to: now
      )

    {:ok, new_points} =
      Timeless.query(:ret_test, "new_metric", %{"id" => "1"},
        from: now - 10_000,
        to: now
      )

    assert old_points == []
    assert length(new_points) == 6
  end

  test "retention cleans orphaned series" do
    now = System.os_time(:second)

    # Write only old data for this series
    Timeless.write(:ret_test, "orphan", %{"id" => "1"}, 1.0,
      timestamp: now - 7200
    )

    Timeless.flush(:ret_test)
    Timeless.enforce_retention(:ret_test)

    info = Timeless.info(:ret_test)
    assert info.series_count == 0
  end
end
