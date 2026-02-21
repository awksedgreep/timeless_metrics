defmodule TimelessMetrics.AdaptiveFlushTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_adaptive_flush_test_#{System.os_time(:millisecond)}"

  setup do
    # Use a long flush interval so we control when adaptation happens via manual flush
    start_supervised!(
      {TimelessMetrics,
       name: :adaptive_test,
       data_dir: @data_dir,
       buffer_shards: 1,
       flush_interval: :timer.seconds(60),
       flush_threshold: 10_000}
    )

    on_exit(fn -> File.rm_rf!(@data_dir) end)
    :ok
  end

  test "threshold stays at default under low write rate" do
    shard = :adaptive_test_shard_0

    now = System.os_time(:second)

    for i <- 1..100 do
      TimelessMetrics.write(:adaptive_test, "low_rate", %{"h" => "a"}, 1.0,
        timestamp: now + i
      )
    end

    # Wait so the rate calculation spans enough time to show a low rate
    # 100 points over 500ms = 200 pts/sec → stays at default
    Process.sleep(500)

    # Trigger flush + adaptation
    send(Process.whereis(shard), :flush)
    Process.sleep(50)

    threshold = TimelessMetrics.Buffer.flush_threshold(shard)
    assert threshold == 10_000
  end

  test "threshold increases under high write rate" do
    shard = :adaptive_test_shard_0

    now = System.os_time(:second)

    # Write a large batch to simulate high rate
    entries =
      for i <- 1..50_000 do
        {"high_rate", %{"h" => "b"}, 1.0, now + i}
      end

    TimelessMetrics.write_batch(:adaptive_test, entries)

    # Small delay, then trigger flush. 50K points in ~100ms = 500K pts/sec
    Process.sleep(100)
    send(Process.whereis(shard), :flush)
    Process.sleep(50)

    threshold = TimelessMetrics.Buffer.flush_threshold(shard)
    assert threshold > 10_000, "Expected threshold to increase above default 10K, got #{threshold}"
  end

  test "threshold resets back to default when rate drops" do
    shard = :adaptive_test_shard_0

    now = System.os_time(:second)

    # First: high rate to push threshold up
    entries =
      for i <- 1..50_000 do
        {"reset_test", %{"h" => "c"}, 1.0, now + i}
      end

    TimelessMetrics.write_batch(:adaptive_test, entries)
    Process.sleep(100)
    send(Process.whereis(shard), :flush)
    Process.sleep(50)

    high_threshold = TimelessMetrics.Buffer.flush_threshold(shard)
    assert high_threshold > 10_000

    # Then: no writes, wait, trigger adaptation again
    Process.sleep(500)
    send(Process.whereis(shard), :flush)
    Process.sleep(50)

    low_threshold = TimelessMetrics.Buffer.flush_threshold(shard)
    assert low_threshold == 10_000, "Expected threshold to reset to 10K, got #{low_threshold}"
  end
end
