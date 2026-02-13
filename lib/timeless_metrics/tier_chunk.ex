defmodule TimelessMetrics.TierChunk do
  @moduledoc """
  Codec for compressed tier segments.

  Encodes lists of aggregate buckets into zstd-compressed binary blobs,
  replacing individual SQLite rows with packed chunks for ~5-10x storage savings.

  ## Binary Format

      <<"TC", version::8, agg_bitmask::8, bucket_count::16, buckets::binary>>

  Each bucket is `timestamp::int64` followed by aggregate values in bitmask order.
  Floats are `float64`, count is `int64`. The entire binary is zstd-compressed.

  ## Aggregate Bitmask

      bit 0: avg    bit 3: count
      bit 1: min    bit 4: sum
      bit 2: max    bit 5: last
  """

  import Bitwise

  @magic "TC"
  @version 1

  @aggregate_bits [avg: 0, min: 1, max: 2, count: 3, sum: 4, last: 5]
  @bit_to_aggregate Enum.into(@aggregate_bits, %{}, fn {k, v} -> {v, k} end)
  @aggregate_to_bit Map.new(@aggregate_bits)

  @doc """
  Encode a list of bucket maps into a compressed blob.

  ## Parameters

    * `buckets` — list of maps, each with `:bucket` (timestamp) key and
      aggregate keys (`:avg`, `:min`, `:max`, `:count`, `:sum`, `:last`)
    * `aggregates` — list of aggregate atoms to encode

  ## Example

      buckets = [
        %{bucket: 1706000000, avg: 73.2, min: 50.1, max: 95.3, count: 12, sum: 878.4, last: 71.0},
        %{bucket: 1706003600, avg: 68.1, min: 45.0, max: 89.7, count: 12, sum: 817.2, last: 65.3}
      ]

      blob = TimelessMetrics.TierChunk.encode(buckets, [:avg, :min, :max, :count, :sum, :last])
  """
  def encode(buckets, aggregates, compression_level \\ 9)

  def encode([], _aggregates, compression_level), do: encode_raw(<<>>, 0, 0, compression_level)

  def encode(buckets, aggregates, compression_level)
      when is_list(buckets) and is_list(aggregates) do
    bitmask = to_bitmask(aggregates)
    ordered = ordered_aggregates(bitmask)
    sorted = Enum.sort_by(buckets, & &1.bucket)

    body =
      Enum.map(sorted, fn bucket ->
        [<<bucket.bucket::signed-64>> | encode_values(bucket, ordered)]
      end)
      |> IO.iodata_to_binary()

    encode_raw(body, bitmask, length(sorted), compression_level)
  end

  defp encode_raw(body, bitmask, count, compression_level) do
    :ezstd.compress(
      <<@magic, @version::8, bitmask::8, count::16, body::binary>>,
      compression_level
    )
  end

  @doc """
  Decode a compressed blob back to aggregate metadata and bucket maps.

  Returns `{aggregates, buckets}` where `aggregates` is the list of
  aggregate atoms and `buckets` is a sorted list of maps.
  """
  def decode(blob) when is_binary(blob) do
    <<@magic, @version::8, bitmask::8, count::16, rest::binary>> = :ezstd.decompress(blob)
    ordered = ordered_aggregates(bitmask)
    buckets = decode_buckets(rest, ordered, count, [])
    {ordered, buckets}
  end

  @doc """
  Merge new buckets into an existing compressed blob.

  New buckets overwrite existing ones at the same timestamp. If `existing`
  is `nil`, equivalent to `encode/2`.
  """
  def merge(existing_blob, new_buckets, aggregates, compression_level \\ 9)

  def merge(nil, new_buckets, aggregates, compression_level) do
    encode(new_buckets, aggregates, compression_level)
  end

  def merge(existing_blob, new_buckets, aggregates, compression_level) do
    {_aggs, existing_buckets} = decode(existing_blob)

    merged =
      existing_buckets
      |> Map.new(&{&1.bucket, &1})
      |> then(fn map ->
        Enum.reduce(new_buckets, map, fn bucket, acc ->
          Map.put(acc, bucket.bucket, bucket)
        end)
      end)
      |> Map.values()

    encode(merged, aggregates, compression_level)
  end

  @doc """
  Return the number of buckets in a compressed blob without fully decoding.
  """
  def bucket_count(blob) when is_binary(blob) do
    <<@magic, @version::8, _bitmask::8, count::16, _rest::binary>> = :ezstd.decompress(blob)
    count
  end

  # --- Encoding helpers ---

  defp encode_values(bucket, [:count | rest]) do
    val = Map.get(bucket, :count, 0)
    [<<trunc(val)::signed-64>> | encode_values(bucket, rest)]
  end

  defp encode_values(bucket, [agg | rest]) do
    val = Map.get(bucket, agg, 0.0)
    [<<val + 0.0::float-64>> | encode_values(bucket, rest)]
  end

  defp encode_values(_bucket, []), do: []

  # --- Decoding helpers ---

  defp decode_buckets(<<>>, _ordered, 0, acc), do: Enum.reverse(acc)

  defp decode_buckets(data, ordered, remaining, acc) do
    <<ts::signed-64, rest::binary>> = data
    {bucket, rest2} = decode_values(rest, ordered, %{bucket: ts})
    decode_buckets(rest2, ordered, remaining - 1, [bucket | acc])
  end

  defp decode_values(data, [], bucket), do: {bucket, data}

  defp decode_values(data, [:count | rest], bucket) do
    <<val::signed-64, remaining::binary>> = data
    decode_values(remaining, rest, Map.put(bucket, :count, val))
  end

  defp decode_values(data, [agg | rest], bucket) do
    <<val::float-64, remaining::binary>> = data
    decode_values(remaining, rest, Map.put(bucket, agg, val))
  end

  # --- Bitmask helpers ---

  defp to_bitmask(aggregates) do
    Enum.reduce(aggregates, 0, fn agg, mask ->
      mask ||| 1 <<< Map.fetch!(@aggregate_to_bit, agg)
    end)
  end

  defp ordered_aggregates(bitmask) do
    for bit <- 0..5, (bitmask &&& 1 <<< bit) != 0, do: Map.fetch!(@bit_to_aggregate, bit)
  end
end
