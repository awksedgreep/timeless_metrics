defmodule TimelessMetrics.Actor.SeriesServerTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.Actor.SeriesServer

  @data_dir "/tmp/series_server_test_#{System.os_time(:millisecond)}"

  setup do
    # Start a unique registry and dynamic supervisor for each test
    test_id = System.unique_integer([:positive])
    registry_name = :"test_series_registry_#{test_id}"
    sup_name = :"test_series_sup_#{test_id}"
    db_name = :"test_series_db_#{test_id}"

    data_dir = "#{@data_dir}_#{test_id}"
    File.mkdir_p!(data_dir)

    start_supervised!({Registry, keys: :unique, name: registry_name})
    start_supervised!({DynamicSupervisor, name: sup_name, strategy: :one_for_one})
    start_supervised!({TimelessMetrics.DB, name: db_name, data_dir: data_dir})

    on_exit(fn -> File.rm_rf!(data_dir) end)

    %{
      registry: registry_name,
      dynamic_sup: sup_name,
      data_dir: data_dir,
      store: :"test_store_#{test_id}",
      db: db_name
    }
  end

  defp start_server(ctx, opts \\ []) do
    series_id = Keyword.get(opts, :series_id, 1)

    server_opts =
      Keyword.merge(
        [
          series_id: series_id,
          metric_name: "cpu",
          labels: %{"host" => "web-1"},
          store: ctx.store,
          data_dir: ctx.data_dir,
          registry: ctx.registry,
          block_size: Keyword.get(opts, :block_size, 1000),
          max_blocks: Keyword.get(opts, :max_blocks, 100),
          compression: :zstd,
          flush_interval: 600_000
        ],
        opts
      )

    child_spec = %{
      id: {:series, series_id},
      start: {SeriesServer, :start_link, [server_opts]},
      restart: :transient
    }

    {:ok, pid} = DynamicSupervisor.start_child(ctx.dynamic_sup, child_spec)
    pid
  end

  test "write single point and query back", ctx do
    pid = start_server(ctx)
    now = System.os_time(:second)

    GenServer.cast(pid, {:write, now, 42.0})
    # Give cast time to process
    Process.sleep(10)

    {:ok, points} = GenServer.call(pid, {:query_raw, now - 1, now + 1})
    assert [{^now, 42.0}] = points
  end

  test "write batch and query range", ctx do
    pid = start_server(ctx)
    now = System.os_time(:second)

    batch = for i <- 0..9, do: {now + i, i * 1.0}
    GenServer.cast(pid, {:write_batch, batch})
    Process.sleep(10)

    {:ok, points} = GenServer.call(pid, {:query_raw, now, now + 9})
    assert length(points) == 10
    assert {^now, 0.0} = List.first(points)
  end

  test "block compression triggers at block_size threshold", ctx do
    pid = start_server(ctx, block_size: 10)
    now = System.os_time(:second)

    # Write 10 points to trigger compression
    for i <- 0..9 do
      GenServer.cast(pid, {:write, now + i, i * 1.0})
    end

    Process.sleep(50)

    state = GenServer.call(pid, :state)
    assert state.block_count == 1
    assert state.raw_count == 0
  end

  test "ring buffer retention drops oldest block", ctx do
    pid = start_server(ctx, block_size: 5, max_blocks: 3)
    now = System.os_time(:second)

    # Write 25 points = 5 blocks, but max_blocks is 3
    for i <- 0..24 do
      GenServer.cast(pid, {:write, now + i, i * 1.0})
    end

    Process.sleep(50)

    state = GenServer.call(pid, :state)
    assert state.block_count == 3

    # Oldest block (points 0-4) should have been dropped
    {:ok, points} = GenServer.call(pid, {:query_raw, now, now + 24})

    # Only blocks 2-4 remain (points 10-24)
    first_ts = List.first(points) |> elem(0)
    assert first_ts == now + 10
  end

  test "aggregation in-process: avg", ctx do
    pid = start_server(ctx)
    now = div(System.os_time(:second), 60) * 60

    # Write 6 points within one minute bucket
    for i <- 0..5 do
      GenServer.cast(pid, {:write, now + i * 5, 20.0 + i})
    end

    Process.sleep(10)

    {:ok, buckets} =
      GenServer.call(pid, {:query_aggregate, now - 60, now + 60, :minute, :avg})

    assert length(buckets) >= 1
    {_ts, avg} = List.first(buckets)
    assert_in_delta avg, 22.5, 0.01
  end

  test "aggregation: max, min, sum, count", ctx do
    pid = start_server(ctx)
    now = div(System.os_time(:second), 60) * 60

    for i <- 1..5, do: GenServer.cast(pid, {:write, now + i, i * 10.0})
    Process.sleep(10)

    {:ok, [{_, max_val}]} =
      GenServer.call(pid, {:query_aggregate, now, now + 60, :minute, :max})

    {:ok, [{_, min_val}]} =
      GenServer.call(pid, {:query_aggregate, now, now + 60, :minute, :min})

    {:ok, [{_, sum_val}]} =
      GenServer.call(pid, {:query_aggregate, now, now + 60, :minute, :sum})

    {:ok, [{_, count_val}]} =
      GenServer.call(pid, {:query_aggregate, now, now + 60, :minute, :count})

    assert max_val == 50.0
    assert min_val == 10.0
    assert_in_delta sum_val, 150.0, 0.01
    assert count_val == 5
  end

  test "latest point from raw buffer", ctx do
    pid = start_server(ctx)
    now = System.os_time(:second)

    GenServer.cast(pid, {:write, now, 10.0})
    GenServer.cast(pid, {:write, now + 1, 20.0})
    Process.sleep(10)

    {:ok, {ts, val}} = GenServer.call(pid, :latest)
    assert ts == now + 1
    assert val == 20.0
  end

  test "latest point from compressed block", ctx do
    pid = start_server(ctx, block_size: 5)
    now = System.os_time(:second)

    for i <- 0..4, do: GenServer.cast(pid, {:write, now + i, i * 1.0})
    Process.sleep(50)

    # Verify block was created and raw buffer is empty
    state = GenServer.call(pid, :state)
    assert state.block_count == 1
    assert state.raw_count == 0

    {:ok, {ts, val}} = GenServer.call(pid, :latest)
    assert ts == now + 4
    assert val == 4.0
  end

  test "persistence round-trip: flush, stop, restart, query back", ctx do
    pid = start_server(ctx, block_size: 5, series_id: 42)
    now = System.os_time(:second)

    # Write 7 points: 5 → 1 block + 2 in raw buffer
    for i <- 0..6, do: GenServer.cast(pid, {:write, now + i, i * 10.0})
    Process.sleep(50)

    # Flush to disk
    :ok = GenServer.call(pid, :flush)

    # Stop the process
    GenServer.stop(pid)
    Process.sleep(50)

    # Restart with same series_id and data_dir
    pid2 = start_server(ctx, series_id: 42, block_size: 5)

    {:ok, points} = GenServer.call(pid2, {:query_raw, now, now + 10})
    assert length(points) == 7

    assert {^now, 0.0} = List.first(points)
    last_ts = now + 6
    assert {^last_ts, 60.0} = List.last(points)
  end

  test "latest returns nil for empty series", ctx do
    pid = start_server(ctx)

    {:ok, nil} = GenServer.call(pid, :latest)
  end

  test "query empty range returns empty list", ctx do
    pid = start_server(ctx)
    now = System.os_time(:second)

    GenServer.cast(pid, {:write, now, 42.0})
    Process.sleep(10)

    {:ok, points} = GenServer.call(pid, {:query_raw, now + 100, now + 200})
    assert points == []
  end

  describe "merge compaction" do
    test "merges multiple small blocks into fewer larger ones", ctx do
      # block_size: 5 creates a block every 5 points
      # merge_block_min_count: 2 so merge triggers with >= 2 eligible blocks
      # merge_block_min_age_seconds: 0 so all blocks are immediately eligible
      # merge_block_max_points: 50 so all blocks merge into one
      pid =
        start_server(ctx,
          block_size: 5,
          merge_block_min_count: 2,
          merge_block_min_age_seconds: 0,
          merge_block_max_points: 50
        )

      # Use timestamps in the past so age check passes
      base = System.os_time(:second) - 1000

      # Write 20 points = 4 blocks of 5
      for i <- 0..19 do
        GenServer.cast(pid, {:write, base + i, i * 1.0})
      end

      Process.sleep(50)

      state_before = GenServer.call(pid, :state)
      assert state_before.block_count == 4
      assert state_before.raw_count == 0

      # Trigger merge
      assert :ok = GenServer.call(pid, :merge_blocks)

      state_after = GenServer.call(pid, :state)
      # 4 blocks of 5 points → 1 merged block of 20 points
      assert state_after.block_count == 1

      # All points still queryable
      {:ok, points} = GenServer.call(pid, {:query_raw, base, base + 19})
      assert length(points) == 20
    end

    test "returns :noop when below min block count", ctx do
      pid =
        start_server(ctx,
          block_size: 5,
          merge_block_min_count: 4,
          merge_block_min_age_seconds: 0,
          merge_block_max_points: 50
        )

      now = System.os_time(:second)

      # Write 10 points = 2 blocks, but min_count is 4
      for i <- 0..9 do
        GenServer.cast(pid, {:write, now + i, i * 1.0})
      end

      Process.sleep(50)

      state = GenServer.call(pid, :state)
      assert state.block_count == 2

      assert :noop = GenServer.call(pid, :merge_blocks)
    end

    test "respects age threshold — only merges old blocks", ctx do
      pid =
        start_server(ctx,
          block_size: 5,
          merge_block_min_count: 2,
          merge_block_min_age_seconds: 300,
          merge_block_max_points: 50
        )

      now = System.os_time(:second)

      # Write 20 points = 4 blocks, all with recent timestamps
      for i <- 0..19 do
        GenServer.cast(pid, {:write, now + i, i * 1.0})
      end

      Process.sleep(50)

      state = GenServer.call(pid, :state)
      assert state.block_count == 4

      # All blocks are "recent" (within 300s) so merge should noop
      assert :noop = GenServer.call(pid, :merge_blocks)
    end

    test "preserves query correctness after merge", ctx do
      pid =
        start_server(ctx,
          block_size: 5,
          merge_block_min_count: 2,
          merge_block_min_age_seconds: 0,
          merge_block_max_points: 50
        )

      base = System.os_time(:second) - 1000

      # Write 30 points = 6 blocks
      for i <- 0..29 do
        GenServer.cast(pid, {:write, base + i, i * 2.0})
      end

      Process.sleep(50)

      # Query before merge
      {:ok, points_before} = GenServer.call(pid, {:query_raw, base, base + 29})
      assert length(points_before) == 30

      # Merge
      assert :ok = GenServer.call(pid, :merge_blocks)

      # Query after merge — same results
      {:ok, points_after} = GenServer.call(pid, {:query_raw, base, base + 29})
      assert points_after == points_before

      # Aggregation still works
      {:ok, buckets} =
        GenServer.call(pid, {:query_aggregate, base, base + 29, :minute, :avg})

      assert length(buckets) >= 1
    end

    test "batches into multiple merged blocks when exceeding max_points", ctx do
      pid =
        start_server(ctx,
          block_size: 5,
          merge_block_min_count: 2,
          merge_block_min_age_seconds: 0,
          merge_block_max_points: 15
        )

      base = System.os_time(:second) - 1000

      # Write 30 points = 6 blocks of 5
      for i <- 0..29 do
        GenServer.cast(pid, {:write, base + i, i * 1.0})
      end

      Process.sleep(50)

      state_before = GenServer.call(pid, :state)
      assert state_before.block_count == 6

      assert :ok = GenServer.call(pid, :merge_blocks)

      state_after = GenServer.call(pid, :state)
      # 6 blocks of 5 → 2 blocks of 15 each
      assert state_after.block_count == 2

      # All 30 points still queryable
      {:ok, points} = GenServer.call(pid, {:query_raw, base, base + 29})
      assert length(points) == 30
    end

    test "persistence round-trip after merge", ctx do
      pid =
        start_server(ctx,
          block_size: 5,
          series_id: 99,
          merge_block_min_count: 2,
          merge_block_min_age_seconds: 0,
          merge_block_max_points: 50
        )

      base = System.os_time(:second) - 1000

      for i <- 0..19 do
        GenServer.cast(pid, {:write, base + i, i * 1.0})
      end

      Process.sleep(50)

      # Merge then flush
      assert :ok = GenServer.call(pid, :merge_blocks)
      :ok = GenServer.call(pid, :flush)

      state_merged = GenServer.call(pid, :state)
      assert state_merged.block_count == 1

      # Stop and restart
      GenServer.stop(pid)
      Process.sleep(50)

      pid2 =
        start_server(ctx,
          block_size: 5,
          series_id: 99,
          merge_block_min_count: 2,
          merge_block_min_age_seconds: 0,
          merge_block_max_points: 50
        )

      {:ok, points} = GenServer.call(pid2, {:query_raw, base, base + 19})
      assert length(points) == 20

      state_reloaded = GenServer.call(pid2, :state)
      assert state_reloaded.block_count == 1
    end
  end
end
