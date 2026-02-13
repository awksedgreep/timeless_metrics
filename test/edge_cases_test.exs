defmodule TimelessMetrics.EdgeCasesTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_edge_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!({TimelessMetrics, name: :edge_test, data_dir: @data_dir, buffer_shards: 1})

    on_exit(fn ->
      :persistent_term.erase({TimelessMetrics, :edge_test, :schema})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "query on empty store returns empty list" do
    {:ok, points} = TimelessMetrics.query(:edge_test, "nonexistent", %{"id" => "1"})
    assert points == []
  end

  test "latest on empty store returns nil" do
    {:ok, result} = TimelessMetrics.latest(:edge_test, "nonexistent", %{"id" => "1"})
    assert result == nil
  end

  test "query_aggregate on empty store returns empty list" do
    {:ok, buckets} =
      TimelessMetrics.query_aggregate(:edge_test, "nonexistent", %{"id" => "1"},
        from: 0,
        to: System.os_time(:second),
        bucket: :hour,
        aggregate: :avg
      )

    assert buckets == []
  end

  test "single point write and query" do
    now = System.os_time(:second)
    TimelessMetrics.write(:edge_test, "solo", %{"id" => "1"}, 42.0, timestamp: now)
    TimelessMetrics.flush(:edge_test)

    {:ok, points} = TimelessMetrics.query(:edge_test, "solo", %{"id" => "1"}, from: now - 60, to: now + 60)
    assert [{^now, 42.0}] = points
  end

  test "duplicate timestamps are preserved" do
    now = System.os_time(:second)

    TimelessMetrics.write(:edge_test, "dup", %{"id" => "1"}, 10.0, timestamp: now)
    TimelessMetrics.write(:edge_test, "dup", %{"id" => "1"}, 20.0, timestamp: now)
    TimelessMetrics.flush(:edge_test)

    {:ok, points} = TimelessMetrics.query(:edge_test, "dup", %{"id" => "1"}, from: now - 60, to: now + 60)
    # Both points should be stored (gorilla allows duplicate timestamps)
    assert length(points) == 2
  end

  test "very large values" do
    now = System.os_time(:second)

    TimelessMetrics.write(:edge_test, "big", %{"id" => "1"}, 1.0e15, timestamp: now)
    TimelessMetrics.write(:edge_test, "big", %{"id" => "1"}, -1.0e15, timestamp: now + 1)
    TimelessMetrics.write(:edge_test, "big", %{"id" => "1"}, 0.0, timestamp: now + 2)
    TimelessMetrics.flush(:edge_test)

    {:ok, points} = TimelessMetrics.query(:edge_test, "big", %{"id" => "1"}, from: now - 60, to: now + 60)
    assert length(points) == 3
    [{_, v1}, {_, v2}, {_, v3}] = points
    assert_in_delta v1, 1.0e15, 1.0e10
    assert_in_delta v2, -1.0e15, 1.0e10
    assert_in_delta v3, 0.0, 0.001
  end

  test "zero values" do
    now = System.os_time(:second)

    for i <- 0..9 do
      TimelessMetrics.write(:edge_test, "zeros", %{"id" => "1"}, 0.0, timestamp: now + i)
    end

    TimelessMetrics.flush(:edge_test)

    {:ok, points} = TimelessMetrics.query(:edge_test, "zeros", %{"id" => "1"}, from: now - 60, to: now + 60)
    assert length(points) == 10
    assert Enum.all?(points, fn {_ts, v} -> v == 0.0 end)
  end

  test "empty labels map" do
    now = System.os_time(:second)

    TimelessMetrics.write(:edge_test, "no_labels", %{}, 99.0, timestamp: now)
    TimelessMetrics.flush(:edge_test)

    {:ok, points} = TimelessMetrics.query(:edge_test, "no_labels", %{}, from: now - 60, to: now + 60)
    assert [{^now, 99.0}] = points
  end

  test "many series with same metric name" do
    now = System.os_time(:second)

    for i <- 1..100 do
      TimelessMetrics.write(:edge_test, "shared_metric", %{"id" => "#{i}"}, i * 1.0, timestamp: now)
    end

    TimelessMetrics.flush(:edge_test)

    # Each should be its own series
    info = TimelessMetrics.info(:edge_test)
    assert info.series_count == 100

    # Query one specific series
    {:ok, points} = TimelessMetrics.query(:edge_test, "shared_metric", %{"id" => "50"}, from: now - 60, to: now + 60)
    assert [{^now, 50.0}] = points
  end

  test "compression round-trip with varied patterns" do
    now = System.os_time(:second)

    patterns = [
      {"constant", fn _i -> 42.0 end},
      {"counter", fn i -> i * 1.0 end},
      {"sine", fn i -> :math.sin(i / 10) * 50 + 50 end},
      {"random", fn _i -> :rand.uniform() * 100 end}
    ]

    Enum.each(patterns, fn {name, gen_fn} ->
      expected =
        for i <- 0..99 do
          ts = now - 500 + i * 5
          val = gen_fn.(i)
          TimelessMetrics.write(:edge_test, "pattern_#{name}", %{"t" => "1"}, val, timestamp: ts)
          {ts, val}
        end

      TimelessMetrics.flush(:edge_test)

      {:ok, actual} = TimelessMetrics.query(:edge_test, "pattern_#{name}", %{"t" => "1"}, from: now - 600, to: now)
      assert length(actual) == 100, "Pattern #{name}: expected 100 points, got #{length(actual)}"

      Enum.zip(expected, actual)
      |> Enum.each(fn {{exp_ts, exp_val}, {act_ts, act_val}} ->
        assert exp_ts == act_ts, "Pattern #{name}: timestamp mismatch"
        assert_in_delta exp_val, act_val, 0.01, "Pattern #{name}: value mismatch at ts=#{exp_ts}"
      end)
    end)
  end

  test "info includes tier stats" do
    info = TimelessMetrics.info(:edge_test)

    assert is_map(info.tiers)
    assert Map.has_key?(info.tiers, :hourly)
    assert Map.has_key?(info.tiers, :daily)
    assert Map.has_key?(info.tiers, :monthly)
    assert info.tiers.hourly.rows >= 0
    assert is_integer(info.tiers.hourly.watermark)
    assert info.buffer_shards >= 1
    assert info.buffer_points >= 0
    assert info.bytes_per_point >= 0
  end

  test "rollup with no data is a no-op" do
    # Should not crash
    assert :ok = TimelessMetrics.rollup(:edge_test)
  end

  test "enforce_retention with no data is a no-op" do
    assert :ok = TimelessMetrics.enforce_retention(:edge_test)
  end

  # --- Phase 4: Numerical edge cases ---

  test "extreme float values survive compression round-trip" do
    now = System.os_time(:second)

    # IEEE 754 extremes
    extreme_values = [
      1.7976931348623157e+308,   # max float64
      -1.7976931348623157e+308,  # min float64
      5.0e-324,                  # smallest positive subnormal
      1.0e-100,                  # small positive
      -1.0e-200,                 # small negative
      0.0                        # exact zero
    ]

    for {val, i} <- Enum.with_index(extreme_values) do
      TimelessMetrics.write(:edge_test, "extreme_floats", %{"id" => "1"}, val, timestamp: now + i)
    end

    TimelessMetrics.flush(:edge_test)

    {:ok, points} =
      TimelessMetrics.query(:edge_test, "extreme_floats", %{"id" => "1"},
        from: now - 1,
        to: now + 10
      )

    assert length(points) == length(extreme_values)

    Enum.zip(extreme_values, Enum.map(points, &elem(&1, 1)))
    |> Enum.each(fn {expected, actual} ->
      assert actual == expected,
        "Extreme float #{expected} not preserved: got #{actual}"
    end)
  end

  test "infinity-scale values don't crash aggregation" do
    now = System.os_time(:second)

    # Values near IEEE 754 limits that exercise edge cases
    TimelessMetrics.write(:edge_test, "inf_agg", %{"id" => "1"}, 1.0e308, timestamp: now)
    TimelessMetrics.write(:edge_test, "inf_agg", %{"id" => "1"}, 42.0, timestamp: now + 1)
    TimelessMetrics.write(:edge_test, "inf_agg", %{"id" => "1"}, -1.0e308, timestamp: now + 2)
    TimelessMetrics.flush(:edge_test)

    for agg <- [:avg, :min, :max, :sum, :count, :last, :first] do
      {:ok, results} =
        TimelessMetrics.query_aggregate_multi(:edge_test, "inf_agg", %{"id" => "1"},
          from: now - 1,
          to: now + 10,
          bucket: {3600, :seconds},
          aggregate: agg
        )

      assert length(results) == 1, "#{agg} returned no results"
      series = List.first(results)
      [{_ts, val}] = series.data
      assert is_number(val), "#{agg} returned non-number: #{inspect(val)}"
    end
  end

  test "negative values in all aggregate functions" do
    now = System.os_time(:second)

    values = [-10.0, -20.0, -5.0, -30.0, -15.0]

    for {val, i} <- Enum.with_index(values) do
      TimelessMetrics.write(:edge_test, "neg_agg", %{"id" => "1"}, val, timestamp: now + i)
    end

    TimelessMetrics.flush(:edge_test)

    expectations = [
      {:avg, -16.0},
      {:min, -30.0},
      {:max, -5.0},
      {:sum, -80.0},
      {:count, 5},
      {:last, -15.0},
      {:first, -10.0}
    ]

    for {agg, expected} <- expectations do
      {:ok, results} =
        TimelessMetrics.query_aggregate_multi(:edge_test, "neg_agg", %{"id" => "1"},
          from: now - 1,
          to: now + 10,
          bucket: {3600, :seconds},
          aggregate: agg
        )

      series = List.first(results)
      [{_ts, val}] = series.data

      assert_in_delta val, expected, 0.01,
        "#{agg} of negatives: expected #{expected}, got #{val}"
    end
  end
end
