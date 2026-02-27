defmodule TimelessMetrics.Actor.TextCodecTest do
  use ExUnit.Case, async: true

  alias TimelessMetrics.Actor.TextCodec

  test "empty input compresses and decompresses" do
    {:ok, compressed} = TextCodec.compress([])
    {:ok, points} = TextCodec.decompress(compressed)
    assert points == []
  end

  test "single value repeated many times (RLE effectiveness)" do
    points = for i <- 1..100, do: {1_000_000 + i, "Cisco IOS 15.2"}

    {:ok, compressed} = TextCodec.compress(points)
    {:ok, decompressed} = TextCodec.decompress(compressed)

    assert decompressed == points
    # Compressed should be much smaller than naive encoding
    naive_size = 100 * (8 + 14)
    assert byte_size(compressed) < naive_size
  end

  test "multiple distinct values" do
    points = [
      {1000, "up"},
      {1001, "up"},
      {1002, "down"},
      {1003, "down"},
      {1004, "down"},
      {1005, "up"}
    ]

    {:ok, compressed} = TextCodec.compress(points)
    {:ok, decompressed} = TextCodec.decompress(compressed)

    assert decompressed == points
  end

  test "round-trip fidelity — timestamps and values preserved exactly" do
    points = [
      {1_700_000_000, "firmware v3.2.1"},
      {1_700_000_060, "firmware v3.2.1"},
      {1_700_000_120, "firmware v3.2.2"},
      {1_700_000_180, "firmware v3.2.2"},
      {1_700_000_240, "firmware v3.2.2"}
    ]

    {:ok, compressed} = TextCodec.compress(points)
    {:ok, decompressed} = TextCodec.decompress(compressed)

    assert decompressed == points
  end

  test "unicode strings" do
    points = [
      {1000, "温度センサー"},
      {1001, "温度センサー"},
      {1002, "Ñoño résumé 🌡️"}
    ]

    {:ok, compressed} = TextCodec.compress(points)
    {:ok, decompressed} = TextCodec.decompress(compressed)

    assert decompressed == points
  end

  test "empty strings" do
    points = [
      {1000, ""},
      {1001, ""},
      {1002, "hello"},
      {1003, ""}
    ]

    {:ok, compressed} = TextCodec.compress(points)
    {:ok, decompressed} = TextCodec.decompress(compressed)

    assert decompressed == points
  end

  test "single point" do
    points = [{42, "only one"}]

    {:ok, compressed} = TextCodec.compress(points)
    {:ok, decompressed} = TextCodec.decompress(compressed)

    assert decompressed == points
  end

  test "alternating values (worst case for RLE)" do
    points = for i <- 1..20, do: {1000 + i, if(rem(i, 2) == 0, do: "even", else: "odd")}

    {:ok, compressed} = TextCodec.compress(points)
    {:ok, decompressed} = TextCodec.decompress(compressed)

    assert decompressed == points
  end

  test "long string values" do
    long_str = String.duplicate("A", 10_000)
    points = for i <- 1..5, do: {1000 + i, long_str}

    {:ok, compressed} = TextCodec.compress(points)
    {:ok, decompressed} = TextCodec.decompress(compressed)

    assert decompressed == points
    # zstd should compress the repeated string very well
    assert byte_size(compressed) < 10_000
  end
end
