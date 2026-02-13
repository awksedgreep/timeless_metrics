defmodule TimelessMetrics.ConcurrencyTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_concurrency_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {TimelessMetrics,
       name: :conc_test,
       data_dir: @data_dir,
       buffer_shards: 4}
    )

    on_exit(fn ->
      :persistent_term.erase({TimelessMetrics, :conc_test, :schema})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "parallel writes to different series don't corrupt data" do
    now = System.os_time(:second)
    series_count = 50
    points_per_series = 20

    # Launch parallel writers — one task per series
    tasks =
      for s <- 1..series_count do
        Task.async(fn ->
          for i <- 0..(points_per_series - 1) do
            TimelessMetrics.write(:conc_test, "metric_#{s}", %{"id" => "#{s}"},
              s * 1.0 + i * 0.01,
              timestamp: now + i
            )
          end
        end)
      end

    Task.await_many(tasks, 30_000)
    TimelessMetrics.flush(:conc_test)

    # Verify each series has exactly the expected number of points
    for s <- 1..series_count do
      {:ok, points} =
        TimelessMetrics.query(:conc_test, "metric_#{s}", %{"id" => "#{s}"},
          from: now - 1,
          to: now + points_per_series + 1
        )

      assert length(points) == points_per_series,
        "series #{s} has #{length(points)} points, expected #{points_per_series}"
    end

    info = TimelessMetrics.info(:conc_test)
    assert info.series_count == series_count
  end

  test "query during active writes returns consistent results" do
    now = System.os_time(:second)

    # Pre-seed some data so queries have something to return
    for i <- 0..9 do
      TimelessMetrics.write(:conc_test, "live_metric", %{"host" => "a"},
        100.0 + i,
        timestamp: now - 100 + i
      )
    end

    TimelessMetrics.flush(:conc_test)

    # Start a writer that continuously pushes data
    writer =
      Task.async(fn ->
        for i <- 0..99 do
          TimelessMetrics.write(:conc_test, "live_metric", %{"host" => "a"},
            200.0 + i,
            timestamp: now + i
          )

          if rem(i, 20) == 0, do: Process.sleep(1)
        end
      end)

    # Concurrently query while writes are happening
    query_results =
      for _ <- 1..10 do
        Task.async(fn ->
          {:ok, points} =
            TimelessMetrics.query(:conc_test, "live_metric", %{"host" => "a"},
              from: now - 200,
              to: now + 200
            )

          # Points should always be sorted and non-empty
          assert length(points) >= 10
          timestamps = Enum.map(points, &elem(&1, 0))
          assert timestamps == Enum.sort(timestamps), "Points not sorted"
          length(points)
        end)
      end

    Task.await(writer, 30_000)
    counts = Task.await_many(query_results, 30_000)

    # All queries should have returned valid results
    assert Enum.all?(counts, &(&1 >= 10))
  end

  test "flush during writes doesn't lose points" do
    now = System.os_time(:second)
    total_points = 200

    # Writer pushes points continuously
    writer =
      Task.async(fn ->
        for i <- 0..(total_points - 1) do
          TimelessMetrics.write(:conc_test, "flush_race", %{"id" => "1"},
            i * 1.0,
            timestamp: now + i
          )

          if rem(i, 50) == 0, do: Process.sleep(1)
        end
      end)

    # Flush multiple times while writer is active
    flusher =
      Task.async(fn ->
        for _ <- 1..5 do
          Process.sleep(5)
          TimelessMetrics.flush(:conc_test)
        end
      end)

    Task.await(writer, 30_000)
    Task.await(flusher, 30_000)

    # Final flush to capture any remaining buffered data
    TimelessMetrics.flush(:conc_test)

    {:ok, points} =
      TimelessMetrics.query(:conc_test, "flush_race", %{"id" => "1"},
        from: now - 1,
        to: now + total_points + 1
      )

    assert length(points) == total_points,
      "Expected #{total_points} points, got #{length(points)} — data lost during flush race"
  end
end
