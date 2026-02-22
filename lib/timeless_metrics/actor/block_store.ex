defmodule TimelessMetrics.Actor.BlockStore do
  @moduledoc """
  Per-series block file codec for the actor engine.

  Pure functions — no GenServer. Each series has a single `.dat` file
  containing compressed blocks and the raw buffer tail.

  File format:
    Header (12 bytes):
      "AM"             — 2 bytes magic
      version          — 1 byte (1)
      block_count      — 4 bytes uint32
      raw_count        — 4 bytes uint32
      flags            — 1 byte reserved

    Per block (repeated block_count times):
      start_ts         — 8 bytes int64
      end_ts           — 8 bytes int64
      point_count      — 4 bytes uint32
      data_len         — 4 bytes uint32
      data             — data_len bytes (GorillaStream + zstd compressed)

    Raw buffer (repeated raw_count times):
      timestamp        — 8 bytes int64
      value            — 8 bytes float64
  """

  @magic "AM"
  @version 1

  @doc """
  Write blocks and raw buffer to a file atomically (tmp + rename).
  """
  def write(path, blocks_queue, raw_buffer) do
    dir = Path.dirname(path)
    File.mkdir_p!(dir)
    tmp_path = path <> ".tmp"

    blocks = :queue.to_list(blocks_queue)
    block_count = length(blocks)
    raw_count = length(raw_buffer)

    header = <<
      @magic::binary,
      @version::8,
      block_count::unsigned-32,
      raw_count::unsigned-32,
      0::8
    >>

    block_data =
      Enum.map(blocks, fn block ->
        data_len = byte_size(block.data)

        <<
          block.start_ts::signed-64,
          block.end_ts::signed-64,
          block.point_count::unsigned-32,
          data_len::unsigned-32,
          block.data::binary
        >>
      end)

    raw_data =
      Enum.map(raw_buffer, fn {ts, val} ->
        <<ts::signed-64, val::float-64>>
      end)

    iodata = [header | block_data] ++ raw_data

    File.write!(tmp_path, iodata)
    File.rename!(tmp_path, path)
    :ok
  end

  @doc """
  Read blocks and raw buffer from a file.

  Returns `{:ok, {blocks_queue, raw_buffer}}` or `{:error, reason}`.
  """
  def read(path) do
    case File.read(path) do
      {:ok, data} ->
        parse(data)

      {:error, :enoent} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse(<<@magic, @version::8, block_count::unsigned-32, raw_count::unsigned-32, _flags::8,
               rest::binary>>) do
    case parse_blocks(rest, block_count, :queue.new()) do
      {:ok, blocks, remaining} ->
        case parse_raw(remaining, raw_count, []) do
          {:ok, raw_buffer} ->
            {:ok, {blocks, raw_buffer}}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse(_data) do
    {:error, :corrupt_header}
  end

  defp parse_blocks(rest, 0, queue), do: {:ok, queue, rest}

  defp parse_blocks(
         <<start_ts::signed-64, end_ts::signed-64, point_count::unsigned-32,
           data_len::unsigned-32, data::binary-size(data_len), rest::binary>>,
         remaining,
         queue
       ) do
    block = %{
      start_ts: start_ts,
      end_ts: end_ts,
      point_count: point_count,
      data: data
    }

    parse_blocks(rest, remaining - 1, :queue.in(block, queue))
  end

  defp parse_blocks(_data, _remaining, _queue) do
    {:error, :corrupt_block}
  end

  defp parse_raw(<<>>, 0, acc), do: {:ok, Enum.reverse(acc)}
  defp parse_raw(_rest, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp parse_raw(<<ts::signed-64, val::float-64, rest::binary>>, remaining, acc) do
    parse_raw(rest, remaining - 1, [{ts, val} | acc])
  end

  defp parse_raw(_data, _remaining, _acc) do
    {:error, :corrupt_raw}
  end

  @doc """
  Build the file path for a series.
  """
  def series_path(data_dir, series_id) do
    Path.join([data_dir, "actor", "series_#{series_id}.dat"])
  end
end
