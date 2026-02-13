defmodule TimelessMetrics.QueryTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_query_test_#{System.os_time(:millisecond)}"

  setup do
    schema = %TimelessMetrics.Schema{
      raw_retention_seconds: 86_400,
      rollup_interval: :timer.hours(1),
      retention_interval: :timer.hours(1),
      tiers: [
        %TimelessMetrics.Schema.Tier{
          name: :hourly,
          resolution_seconds: 3_600,
          aggregates: [:avg, :min, :max, :count, :sum, :last],
          retention_seconds: :forever,
          table_name: "tier_hourly"
        }
      ]
    }

    start_supervised!(
      {TimelessMetrics, name: :query_test, data_dir: @data_dir, buffer_shards: 1, schema: schema}
    )

    on_exit(fn ->
      :persistent_term.erase({TimelessMetrics, :query_test, :schema})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "query_aggregate_multi returns multiple series" do
    now = System.os_time(:second)

    for host <- ["web-1", "web-2", "web-3"] do
      for i <- 0..4 do
        TimelessMetrics.write(:query_test, "cpu", %{"host" => host}, 50.0 + i,
          timestamp: now - 300 + i * 60
        )
      end
    end

    TimelessMetrics.flush(:query_test)

    {:ok, results} =
      TimelessMetrics.query_aggregate_multi(:query_test, "cpu", %{},
        from: now - 600,
        to: now + 60,
        bucket: {3600, :seconds},
        aggregate: :avg
      )

    assert length(results) == 3

    hosts = Enum.map(results, fn %{labels: l} -> l["host"] end) |> Enum.sort()
    assert hosts == ["web-1", "web-2", "web-3"]

    Enum.each(results, fn %{data: data} ->
      assert length(data) >= 1
    end)
  end

  test "query with from > to returns empty" do
    now = System.os_time(:second)

    TimelessMetrics.write(:query_test, "mem", %{"host" => "a"}, 42.0, timestamp: now)
    TimelessMetrics.flush(:query_test)

    {:ok, points} =
      TimelessMetrics.query(:query_test, "mem", %{"host" => "a"},
        from: now + 1000,
        to: now - 1000
      )

    assert points == []
  end

  test "rate calculation across consecutive points" do
    now = System.os_time(:second)

    # Write monotonically increasing counter values: 100, 200, 300, 400, 500
    # 60-second intervals → rate should be ~1.67/sec (100 per 60s)
    for i <- 0..4 do
      TimelessMetrics.write(:query_test, "bytes_total", %{"iface" => "eth0"}, (i + 1) * 100.0,
        timestamp: now - 240 + i * 60
      )
    end

    TimelessMetrics.flush(:query_test)

    {:ok, results} =
      TimelessMetrics.query_aggregate_multi(:query_test, "bytes_total", %{"iface" => "eth0"},
        from: now - 300,
        to: now + 60,
        bucket: {86_400, :seconds},
        aggregate: :rate
      )

    assert length(results) == 1
    series = List.first(results)
    assert length(series.data) == 1

    [{_bucket, rate}] = series.data
    # 400 increase over 240 seconds = ~1.67/sec
    assert_in_delta rate, 400.0 / 240.0, 0.1
  end

  test "latest falls back to tier data when no raw segments" do
    now = System.os_time(:second)
    # Write data in a completed past hour so rollup will process it
    past_hour = div(now, 3600) * 3600 - 3600

    for i <- 0..9 do
      TimelessMetrics.write(:query_test, "fallback_metric", %{"host" => "x"}, 42.0 + i,
        timestamp: past_hour + i * 60
      )
    end

    TimelessMetrics.flush(:query_test)

    # Force a rollup to populate tier data from the completed hour
    TimelessMetrics.rollup(:query_test)

    # Verify tier data was created
    {:ok, tier_data} =
      TimelessMetrics.query_tier(:query_test, :hourly, "fallback_metric", %{"host" => "x"},
        from: past_hour - 1,
        to: past_hour + 3600
      )

    assert length(tier_data) >= 1, "Rollup did not produce tier data"

    # Delete all raw segments to simulate retention expiry
    shard_count = :persistent_term.get({TimelessMetrics, :query_test, :shard_count})

    for i <- 0..(shard_count - 1) do
      builder = :"query_test_builder_#{i}"
      TimelessMetrics.SegmentBuilder.delete_raw_before(builder, now + 86_400)
    end

    # latest should fall back to tier data
    {:ok, result} = TimelessMetrics.latest(:query_test, "fallback_metric", %{"host" => "x"})
    assert result != nil
    {_ts, val} = result
    assert is_number(val)
  end

  test "query spanning rollup watermark stitches raw + tier data" do
    now = System.os_time(:second)
    # Align to hour boundary for clean tier buckets
    hour_start = div(now, 3600) * 3600

    # Write data in the "old" hour (will be covered by rollup)
    for i <- 0..4 do
      TimelessMetrics.write(:query_test, "stitch_metric", %{"host" => "s"}, 10.0 + i,
        timestamp: hour_start - 3600 + i * 60
      )
    end

    # Write data in the "current" hour (raw only)
    for i <- 0..4 do
      TimelessMetrics.write(:query_test, "stitch_metric", %{"host" => "s"}, 50.0 + i,
        timestamp: hour_start + i * 60
      )
    end

    TimelessMetrics.flush(:query_test)

    # Run rollup — this will process the old hour into tier data
    TimelessMetrics.rollup(:query_test)

    # Query spanning both: old tier data + new raw data
    {:ok, results} =
      TimelessMetrics.query_aggregate_multi(:query_test, "stitch_metric", %{"host" => "s"},
        from: hour_start - 3600,
        to: hour_start + 3600,
        bucket: {3600, :seconds},
        aggregate: :avg
      )

    assert length(results) == 1
    series = List.first(results)

    # Should have data from both the old hour (tier) and current hour (raw)
    assert length(series.data) >= 2
    timestamps = Enum.map(series.data, &elem(&1, 0))
    assert Enum.min(timestamps) < hour_start
    assert Enum.max(timestamps) >= hour_start
  end
end
