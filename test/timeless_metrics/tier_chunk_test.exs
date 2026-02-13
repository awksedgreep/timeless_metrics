defmodule TimelessMetrics.TierChunkTest do
  use ExUnit.Case, async: true

  alias TimelessMetrics.TierChunk

  @all_aggregates [:avg, :min, :max, :count, :sum, :last]

  defp sample_bucket(ts, base \\ 70.0) do
    %{
      bucket: ts,
      avg: base + 3.2,
      min: base - 19.9,
      max: base + 25.3,
      count: 12,
      sum: (base + 3.2) * 12,
      last: base + 1.0
    }
  end

  # --- Round-trip encode/decode ---

  test "round-trip with all aggregates" do
    buckets = [
      sample_bucket(1_706_000_000),
      sample_bucket(1_706_003_600, 65.0),
      sample_bucket(1_706_007_200, 80.0)
    ]

    blob = TierChunk.encode(buckets, @all_aggregates)
    assert is_binary(blob)

    {aggs, decoded} = TierChunk.decode(blob)
    assert aggs == @all_aggregates
    assert length(decoded) == 3

    # Verify sorted order
    assert Enum.map(decoded, & &1.bucket) == [1_706_000_000, 1_706_003_600, 1_706_007_200]

    # Verify values round-trip exactly
    for {orig, dec} <- Enum.zip(Enum.sort_by(buckets, & &1.bucket), decoded) do
      assert dec.bucket == orig.bucket
      assert_in_delta dec.avg, orig.avg, 1.0e-10
      assert_in_delta dec.min, orig.min, 1.0e-10
      assert_in_delta dec.max, orig.max, 1.0e-10
      assert dec.count == orig.count
      assert_in_delta dec.sum, orig.sum, 1.0e-10
      assert_in_delta dec.last, orig.last, 1.0e-10
    end
  end

  test "round-trip with subset of aggregates" do
    buckets = [%{bucket: 1_000_000, avg: 42.5, count: 7}]

    blob = TierChunk.encode(buckets, [:avg, :count])
    {aggs, [decoded]} = TierChunk.decode(blob)

    assert aggs == [:avg, :count]
    assert decoded.bucket == 1_000_000
    assert_in_delta decoded.avg, 42.5, 1.0e-10
    assert decoded.count == 7
    refute Map.has_key?(decoded, :min)
    refute Map.has_key?(decoded, :max)
    refute Map.has_key?(decoded, :sum)
    refute Map.has_key?(decoded, :last)
  end

  test "round-trip with single aggregate" do
    buckets = [%{bucket: 500, last: 99.9}]
    blob = TierChunk.encode(buckets, [:last])
    {[:last], [decoded]} = TierChunk.decode(blob)
    assert_in_delta decoded.last, 99.9, 1.0e-10
  end

  test "round-trip preserves negative values" do
    buckets = [
      %{bucket: 100, avg: -42.5, min: -100.0, max: -1.0, count: 5, sum: -212.5, last: -30.0}
    ]

    blob = TierChunk.encode(buckets, @all_aggregates)
    {_aggs, [decoded]} = TierChunk.decode(blob)

    assert_in_delta decoded.avg, -42.5, 1.0e-10
    assert_in_delta decoded.min, -100.0, 1.0e-10
    assert_in_delta decoded.max, -1.0, 1.0e-10
    assert decoded.count == 5
    assert_in_delta decoded.sum, -212.5, 1.0e-10
    assert_in_delta decoded.last, -30.0, 1.0e-10
  end

  test "round-trip with zero values" do
    buckets = [%{bucket: 0, avg: 0.0, min: 0.0, max: 0.0, count: 0, sum: 0.0, last: 0.0}]
    blob = TierChunk.encode(buckets, @all_aggregates)
    {_aggs, [decoded]} = TierChunk.decode(blob)

    assert_in_delta decoded.avg, 0.0, 1.0e-10
    assert decoded.count == 0
  end

  test "round-trip with negative timestamps" do
    buckets = [%{bucket: -86400, avg: 1.0, count: 1}]
    blob = TierChunk.encode(buckets, [:avg, :count])
    {_aggs, [decoded]} = TierChunk.decode(blob)
    assert decoded.bucket == -86400
  end

  # --- Sorting ---

  test "encode sorts buckets by timestamp" do
    buckets = [
      %{bucket: 300, avg: 3.0, count: 3},
      %{bucket: 100, avg: 1.0, count: 1},
      %{bucket: 200, avg: 2.0, count: 2}
    ]

    blob = TierChunk.encode(buckets, [:avg, :count])
    {_aggs, decoded} = TierChunk.decode(blob)

    assert Enum.map(decoded, & &1.bucket) == [100, 200, 300]
    assert Enum.map(decoded, & &1.avg) |> Enum.map(&Float.round(&1, 1)) == [1.0, 2.0, 3.0]
  end

  # --- Empty buckets ---

  test "encode empty list" do
    blob = TierChunk.encode([], @all_aggregates)
    assert is_binary(blob)

    {_aggs, decoded} = TierChunk.decode(blob)
    assert decoded == []
  end

  # --- bucket_count/1 ---

  test "bucket_count without full decode" do
    buckets = for i <- 0..23, do: sample_bucket(1_706_000_000 + i * 3600)
    blob = TierChunk.encode(buckets, @all_aggregates)

    assert TierChunk.bucket_count(blob) == 24
  end

  test "bucket_count of empty blob" do
    blob = TierChunk.encode([], @all_aggregates)
    assert TierChunk.bucket_count(blob) == 0
  end

  # --- Merge ---

  test "merge with nil creates new chunk" do
    buckets = [sample_bucket(1000), sample_bucket(2000)]
    blob = TierChunk.merge(nil, buckets, @all_aggregates)

    {_aggs, decoded} = TierChunk.decode(blob)
    assert length(decoded) == 2
    assert Enum.map(decoded, & &1.bucket) == [1000, 2000]
  end

  test "merge overwrites existing buckets at same timestamp" do
    original = [
      %{bucket: 1000, avg: 10.0, min: 5.0, max: 15.0, count: 10, sum: 100.0, last: 12.0},
      %{bucket: 2000, avg: 20.0, min: 10.0, max: 30.0, count: 10, sum: 200.0, last: 22.0}
    ]

    blob = TierChunk.encode(original, @all_aggregates)

    # Overwrite bucket 1000, keep 2000
    new = [%{bucket: 1000, avg: 99.0, min: 90.0, max: 100.0, count: 50, sum: 4950.0, last: 98.0}]
    merged_blob = TierChunk.merge(blob, new, @all_aggregates)

    {_aggs, decoded} = TierChunk.decode(merged_blob)
    assert length(decoded) == 2

    b1 = Enum.find(decoded, &(&1.bucket == 1000))
    b2 = Enum.find(decoded, &(&1.bucket == 2000))

    # bucket 1000 should have new values
    assert_in_delta b1.avg, 99.0, 1.0e-10
    assert b1.count == 50

    # bucket 2000 should be unchanged
    assert_in_delta b2.avg, 20.0, 1.0e-10
    assert b2.count == 10
  end

  test "merge adds new timestamps to existing chunk" do
    original = [sample_bucket(1000)]
    blob = TierChunk.encode(original, @all_aggregates)

    new = [sample_bucket(2000), sample_bucket(3000)]
    merged_blob = TierChunk.merge(blob, new, @all_aggregates)

    {_aggs, decoded} = TierChunk.decode(merged_blob)
    assert length(decoded) == 3
    assert Enum.map(decoded, & &1.bucket) == [1000, 2000, 3000]
  end

  # --- Compression ---

  test "compression ratio for realistic hourly chunk (24 buckets)" do
    # Simulate 24 hourly buckets of ISP device metrics
    buckets =
      for i <- 0..23 do
        base = 50.0 + :rand.uniform() * 40

        %{
          bucket: 1_706_000_000 + i * 3600,
          avg: base,
          min: base - 10 - :rand.uniform() * 15,
          max: base + 10 + :rand.uniform() * 15,
          count: 12,
          sum: base * 12,
          last: base + (:rand.uniform() - 0.5) * 5
        }
      end

    blob = TierChunk.encode(buckets, @all_aggregates)
    # 24 buckets × (ts + 6 values) × 8 bytes
    uncompressed_size = 24 * (8 + 6 * 8)
    compressed_size = byte_size(blob)
    ratio = uncompressed_size / compressed_size

    # zstd on 24 random floats won't compress much — the real win is
    # fewer SQLite rows (1 chunk row vs 24 individual rows × 78 bytes overhead).
    # Just verify the blob is smaller than raw uncompressed.
    assert compressed_size < uncompressed_size,
           "Expected compression, got #{compressed_size} >= #{uncompressed_size} bytes " <>
             "(#{Float.round(ratio, 1)}x)"
  end

  test "compression ratio for daily chunk (30 buckets)" do
    buckets =
      for i <- 0..29 do
        base = 60.0 + :rand.uniform() * 30

        %{
          bucket: 1_706_000_000 + i * 86400,
          avg: base,
          min: base - 20,
          max: base + 20,
          count: 288,
          sum: base * 288,
          last: base + 1.0
        }
      end

    blob = TierChunk.encode(buckets, @all_aggregates)
    uncompressed_size = 30 * (8 + 6 * 8)
    compressed_size = byte_size(blob)

    assert compressed_size < uncompressed_size,
           "Expected compression, got #{compressed_size} >= #{uncompressed_size} bytes"
  end

  test "bytes per point comparison with old row-based storage" do
    # Old format: ~78 bytes per row (SQLite overhead + 6 aggregates)
    # New format: compressed chunk / bucket_count
    buckets =
      for i <- 0..23 do
        base = 70.0 + :rand.uniform() * 20

        %{
          bucket: 1_706_000_000 + i * 3600,
          avg: base,
          min: base - 15,
          max: base + 15,
          count: 12,
          sum: base * 12,
          last: base
        }
      end

    blob = TierChunk.encode(buckets, @all_aggregates)
    bytes_per_bucket = byte_size(blob) / 24
    old_bytes_per_bucket = 78

    assert bytes_per_bucket < old_bytes_per_bucket,
           "Expected chunk bytes/bucket (#{Float.round(bytes_per_bucket, 1)}) " <>
             "< old row bytes (#{old_bytes_per_bucket})"
  end

  # --- Aggregate ordering (bitmask) ---

  test "different aggregate orderings produce same decode result" do
    bucket = %{bucket: 1000, avg: 1.0, min: 2.0, max: 3.0, count: 4, sum: 5.0, last: 6.0}

    blob1 = TierChunk.encode([bucket], [:avg, :min, :max, :count, :sum, :last])
    blob2 = TierChunk.encode([bucket], [:last, :sum, :count, :max, :min, :avg])

    {_aggs1, [decoded1]} = TierChunk.decode(blob1)
    {_aggs2, [decoded2]} = TierChunk.decode(blob2)

    # Both should decode to the same values
    assert_in_delta decoded1.avg, decoded2.avg, 1.0e-10
    assert_in_delta decoded1.min, decoded2.min, 1.0e-10
    assert_in_delta decoded1.max, decoded2.max, 1.0e-10
    assert decoded1.count == decoded2.count
    assert_in_delta decoded1.sum, decoded2.sum, 1.0e-10
    assert_in_delta decoded1.last, decoded2.last, 1.0e-10
  end

  test "blobs with same aggregates in different order are identical" do
    bucket = %{bucket: 1000, avg: 1.0, count: 4}

    blob1 = TierChunk.encode([bucket], [:avg, :count])
    blob2 = TierChunk.encode([bucket], [:count, :avg])

    # Bitmask is the same regardless of input order, so blobs should be identical
    assert blob1 == blob2
  end

  # --- Large chunk ---

  test "round-trip with 720 buckets (monthly hourly)" do
    buckets =
      for i <- 0..719 do
        %{bucket: 1_706_000_000 + i * 3600, avg: :rand.uniform() * 100, count: 12}
      end

    blob = TierChunk.encode(buckets, [:avg, :count])
    assert TierChunk.bucket_count(blob) == 720

    {[:avg, :count], decoded} = TierChunk.decode(blob)
    assert length(decoded) == 720

    # Verify first and last match
    assert decoded |> hd() |> Map.get(:bucket) == 1_706_000_000
    assert decoded |> List.last() |> Map.get(:bucket) == 1_706_000_000 + 719 * 3600
  end

  # --- Missing values default ---

  test "missing aggregate values default to zero" do
    # Bucket with only :avg, missing :count
    bucket = %{bucket: 1000, avg: 42.0}
    blob = TierChunk.encode([bucket], [:avg, :count])
    {_aggs, [decoded]} = TierChunk.decode(blob)

    assert_in_delta decoded.avg, 42.0, 1.0e-10
    assert decoded.count == 0
  end
end
