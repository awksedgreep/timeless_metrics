defmodule TimelessMetrics.Actor.BlockStoreTest do
  use ExUnit.Case, async: true

  alias TimelessMetrics.Actor.BlockStore

  @data_dir "/tmp/block_store_test_#{System.os_time(:millisecond)}"

  setup do
    File.mkdir_p!(@data_dir)
    on_exit(fn -> File.rm_rf!(@data_dir) end)
    :ok
  end

  test "write and read empty file" do
    path = Path.join(@data_dir, "empty.dat")
    blocks = :queue.new()
    raw_buffer = []

    assert :ok = BlockStore.write(path, blocks, raw_buffer)
    assert {:ok, {read_blocks, read_raw}} = BlockStore.read(path)
    assert :queue.to_list(read_blocks) == []
    assert read_raw == []
  end

  test "write and read with blocks and raw buffer" do
    path = Path.join(@data_dir, "full.dat")

    # Create some blocks
    block1 = %{start_ts: 1000, end_ts: 1099, point_count: 100, data: :crypto.strong_rand_bytes(64)}
    block2 = %{start_ts: 1100, end_ts: 1199, point_count: 100, data: :crypto.strong_rand_bytes(128)}

    blocks = :queue.from_list([block1, block2])
    raw_buffer = [{1200, 42.0}, {1201, 43.5}, {1202, 44.1}]

    assert :ok = BlockStore.write(path, blocks, raw_buffer)
    assert {:ok, {read_blocks, read_raw}} = BlockStore.read(path)

    read_blocks_list = :queue.to_list(read_blocks)
    assert length(read_blocks_list) == 2

    [rb1, rb2] = read_blocks_list
    assert rb1.start_ts == 1000
    assert rb1.end_ts == 1099
    assert rb1.point_count == 100
    assert rb1.data == block1.data

    assert rb2.start_ts == 1100
    assert rb2.end_ts == 1199
    assert rb2.point_count == 100
    assert rb2.data == block2.data

    assert read_raw == [{1200, 42.0}, {1201, 43.5}, {1202, 44.1}]
  end

  test "write and read with blocks only (no raw buffer)" do
    path = Path.join(@data_dir, "blocks_only.dat")

    block = %{start_ts: 500, end_ts: 599, point_count: 50, data: <<1, 2, 3, 4, 5>>}
    blocks = :queue.from_list([block])

    assert :ok = BlockStore.write(path, blocks, [])
    assert {:ok, {read_blocks, read_raw}} = BlockStore.read(path)
    assert length(:queue.to_list(read_blocks)) == 1
    assert read_raw == []
  end

  test "write and read with raw buffer only (no blocks)" do
    path = Path.join(@data_dir, "raw_only.dat")

    raw = [{100, 1.0}, {200, 2.0}]

    assert :ok = BlockStore.write(path, :queue.new(), raw)
    assert {:ok, {read_blocks, read_raw}} = BlockStore.read(path)
    assert :queue.to_list(read_blocks) == []
    assert read_raw == [{100, 1.0}, {200, 2.0}]
  end

  test "read non-existent file returns error" do
    path = Path.join(@data_dir, "nonexistent.dat")
    assert {:error, :not_found} = BlockStore.read(path)
  end

  test "corrupt file returns error" do
    path = Path.join(@data_dir, "corrupt.dat")
    File.write!(path, "garbage data")
    assert {:error, :corrupt_header} = BlockStore.read(path)
  end

  test "atomic write does not corrupt on crash" do
    path = Path.join(@data_dir, "atomic.dat")
    tmp_path = path <> ".tmp"

    block = %{start_ts: 1, end_ts: 2, point_count: 1, data: <<42>>}
    blocks = :queue.from_list([block])
    assert :ok = BlockStore.write(path, blocks, [])

    # The tmp file should not exist after a successful write
    refute File.exists?(tmp_path)
    assert File.exists?(path)
  end

  test "series_path generates correct path" do
    assert BlockStore.series_path("/data", 42) == "/data/actor/series_42.dat"
    assert BlockStore.series_path("/tmp/metrics", 1) == "/tmp/metrics/actor/series_1.dat"
  end

  test "negative timestamps are preserved" do
    path = Path.join(@data_dir, "negative.dat")

    block = %{start_ts: -1000, end_ts: -500, point_count: 10, data: <<0>>}
    blocks = :queue.from_list([block])
    raw = [{-100, -42.5}]

    assert :ok = BlockStore.write(path, blocks, raw)
    assert {:ok, {read_blocks, read_raw}} = BlockStore.read(path)

    [rb] = :queue.to_list(read_blocks)
    assert rb.start_ts == -1000
    assert rb.end_ts == -500
    assert read_raw == [{-100, -42.5}]
  end
end
