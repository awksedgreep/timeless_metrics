defmodule TimelessMetrics.Actor.TextCodec do
  @moduledoc """
  RLE + zstd compression codec for text time-series.

  Groups consecutive points with the same string value into a single entry,
  storing the string once with all associated timestamps. The resulting binary
  is then zstd-compressed.

  Binary format (pre-zstd):

      entry_count    4 bytes  uint32
      Per entry:
        ts_count     4 bytes  uint32
        timestamps   ts_count * 8 bytes (each int64)
        value_len    4 bytes  uint32
        value        value_len bytes (UTF-8 string)
  """

  @doc """
  Compress a list of `{timestamp, string}` points into a binary.

  Points must be sorted by timestamp. Consecutive points with the same
  string value are grouped into a single RLE entry.
  """
  @spec compress([{integer(), String.t()}]) :: {:ok, binary()} | {:error, term()}
  def compress([]), do: {:ok, encode_empty()}

  def compress(points) when is_list(points) do
    entries = rle_encode(points)
    binary = encode_entries(entries)

    case :ezstd.compress(binary) do
      compressed when is_binary(compressed) -> {:ok, compressed}
      {:error, reason} -> {:error, reason}
    end
  rescue
    e -> {:error, e}
  end

  @doc """
  Decompress a binary back into `{timestamp, string}` points.
  """
  @spec decompress(binary()) :: {:ok, [{integer(), String.t()}]} | {:error, term()}
  def decompress(binary) when is_binary(binary) do
    case :ezstd.decompress(binary) do
      decompressed when is_binary(decompressed) ->
        decode_entries(decompressed)

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    e -> {:error, e}
  end

  # --- RLE encoding ---

  defp rle_encode(points) do
    points
    |> Enum.chunk_by(fn {_ts, val} -> val end)
    |> Enum.map(fn chunk ->
      timestamps = Enum.map(chunk, &elem(&1, 0))
      {_ts, value} = hd(chunk)
      {timestamps, value}
    end)
  end

  # --- Binary encoding ---

  defp encode_empty do
    binary = <<0::unsigned-32>>

    case :ezstd.compress(binary) do
      compressed when is_binary(compressed) -> compressed
    end
  end

  defp encode_entries(entries) do
    entry_count = length(entries)

    entry_data =
      Enum.map(entries, fn {timestamps, value} ->
        ts_count = length(timestamps)
        ts_binary = Enum.map(timestamps, fn ts -> <<ts::signed-64>> end)
        value_bytes = :erlang.iolist_to_binary(value)
        value_len = byte_size(value_bytes)

        [
          <<ts_count::unsigned-32>>,
          ts_binary,
          <<value_len::unsigned-32>>,
          value_bytes
        ]
      end)

    :erlang.iolist_to_binary([<<entry_count::unsigned-32>> | entry_data])
  end

  # --- Binary decoding ---

  defp decode_entries(<<entry_count::unsigned-32, rest::binary>>) do
    decode_entries(rest, entry_count, [])
  end

  defp decode_entries(_other) do
    {:error, :corrupt_text_data}
  end

  defp decode_entries(<<>>, 0, acc) do
    points =
      acc
      |> Enum.reverse()
      |> Enum.flat_map(fn {timestamps, value} ->
        Enum.map(timestamps, fn ts -> {ts, value} end)
      end)

    {:ok, points}
  end

  defp decode_entries(_rest, 0, acc) do
    # Trailing data is acceptable
    points =
      acc
      |> Enum.reverse()
      |> Enum.flat_map(fn {timestamps, value} ->
        Enum.map(timestamps, fn ts -> {ts, value} end)
      end)

    {:ok, points}
  end

  defp decode_entries(<<ts_count::unsigned-32, rest::binary>>, remaining, acc) do
    ts_bytes = ts_count * 8

    case rest do
      <<ts_data::binary-size(ts_bytes), value_len::unsigned-32, value::binary-size(value_len),
        more::binary>> ->
        timestamps = decode_timestamps(ts_data, ts_count, [])
        decode_entries(more, remaining - 1, [{timestamps, value} | acc])

      _ ->
        {:error, :corrupt_text_entry}
    end
  end

  defp decode_entries(_data, _remaining, _acc) do
    {:error, :corrupt_text_entry}
  end

  defp decode_timestamps(<<>>, 0, acc), do: Enum.reverse(acc)

  defp decode_timestamps(<<ts::signed-64, rest::binary>>, remaining, acc) do
    decode_timestamps(rest, remaining - 1, [ts | acc])
  end
end
