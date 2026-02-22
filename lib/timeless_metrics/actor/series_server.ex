defmodule TimelessMetrics.Actor.SeriesServer do
  @moduledoc """
  Per-series GenServer for the actor engine.

  Each time series gets its own process. The process owns the entire data
  lifecycle: raw buffer, compression into blocks, ring buffer retention,
  persistence to disk, and query serving.

  Write path is `cast` (fire and forget). Read path is `call` (synchronous).
  """

  use GenServer

  alias TimelessMetrics.Actor.{Aggregation, BlockStore}

  @flush_interval_ms 60_000
  @stale_check_ms 30_000

  defstruct [
    :series_id,
    :metric_name,
    :labels,
    :store,
    :data_dir,
    :registry,
    raw_buffer: [],
    raw_count: 0,
    blocks: :queue.new(),
    block_count: 0,
    max_blocks: 100,
    block_size: 1000,
    compression: :zstd,
    dirty: false,
    flush_ref: nil,
    stale_ref: nil,
    last_write_at: nil
  ]

  # --- Client API ---

  def start_link(opts) do
    registry = Keyword.fetch!(opts, :registry)
    series_id = Keyword.fetch!(opts, :series_id)

    GenServer.start_link(__MODULE__, opts,
      name: {:via, Registry, {registry, series_id}}
    )
  end

  # --- Server ---

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    series_id = Keyword.fetch!(opts, :series_id)
    metric_name = Keyword.fetch!(opts, :metric_name)
    labels = Keyword.fetch!(opts, :labels)
    store = Keyword.fetch!(opts, :store)
    data_dir = Keyword.fetch!(opts, :data_dir)
    registry = Keyword.fetch!(opts, :registry)
    max_blocks = Keyword.get(opts, :max_blocks, 100)
    block_size = Keyword.get(opts, :block_size, 1000)
    compression = Keyword.get(opts, :compression, :zstd)
    flush_interval = Keyword.get(opts, :flush_interval, @flush_interval_ms)

    state = %__MODULE__{
      series_id: series_id,
      metric_name: metric_name,
      labels: labels,
      store: store,
      data_dir: data_dir,
      registry: registry,
      max_blocks: max_blocks,
      block_size: block_size,
      compression: compression
    }

    # Load existing data from disk
    state = load_from_disk(state)

    # Schedule periodic flush and stale check
    flush_ref = Process.send_after(self(), :flush_to_disk, flush_interval)
    stale_ref = Process.send_after(self(), :maybe_compress_stale, @stale_check_ms)

    {:ok, %{state | flush_ref: flush_ref, stale_ref: stale_ref}}
  end

  @impl true
  def handle_cast({:write, ts, val}, state) do
    state = %{
      state
      | raw_buffer: [{ts, val} | state.raw_buffer],
        raw_count: state.raw_count + 1,
        dirty: true,
        last_write_at: System.monotonic_time(:millisecond)
    }

    state =
      if state.raw_count >= state.block_size do
        compress_buffer(state)
      else
        state
      end

    {:noreply, state}
  end

  def handle_cast({:write_batch, points}, state) do
    state =
      Enum.reduce(points, state, fn {ts, val}, acc ->
        %{
          acc
          | raw_buffer: [{ts, val} | acc.raw_buffer],
            raw_count: acc.raw_count + 1
        }
      end)

    state = %{state | dirty: true, last_write_at: System.monotonic_time(:millisecond)}

    state =
      if state.raw_count >= state.block_size do
        compress_buffer(state)
      else
        state
      end

    {:noreply, state}
  end

  @impl true
  def handle_call({:query_raw, from, to}, _from_pid, state) do
    points = query_raw_points(state, from, to)
    {:reply, {:ok, points}, state}
  end

  def handle_call({:query_aggregate, from, to, bucket_spec, agg_fn}, _from_pid, state) do
    bucket_seconds = Aggregation.bucket_to_seconds(bucket_spec)
    points = query_raw_points(state, from, to)
    buckets = Aggregation.bucket_points(points, bucket_seconds, agg_fn)
    {:reply, {:ok, buckets}, state}
  end

  def handle_call(:latest, _from_pid, state) do
    result = get_latest(state)
    {:reply, {:ok, result}, state}
  end

  def handle_call(:flush, _from_pid, state) do
    state = flush_to_disk(state)
    {:reply, :ok, state}
  end

  def handle_call(:state, _from_pid, state) do
    {:reply, state, state}
  end

  def handle_call(:stats, _from_pid, state) do
    compressed_bytes =
      state.blocks
      |> :queue.to_list()
      |> Enum.reduce(0, fn block, acc -> acc + byte_size(block.data) end)

    oldest_ts =
      case :queue.peek(state.blocks) do
        {:value, block} -> block.start_ts
        :empty ->
          case state.raw_buffer do
            [] -> nil
            buf -> buf |> Enum.map(&elem(&1, 0)) |> Enum.min()
          end
      end

    newest_ts =
      case state.raw_buffer do
        [] ->
          case :queue.peek_r(state.blocks) do
            {:value, block} -> block.end_ts
            :empty -> nil
          end
        buf ->
          buf |> Enum.map(&elem(&1, 0)) |> Enum.max()
      end

    stats = %{
      block_count: state.block_count,
      raw_count: state.raw_count,
      compressed_bytes: compressed_bytes,
      oldest_ts: oldest_ts,
      newest_ts: newest_ts
    }

    {:reply, stats, state}
  end

  def handle_call({:compute_daily, from, to}, _from_pid, state) do
    points = query_raw_points(state, from, to)

    result =
      if points == [] do
        nil
      else
        values = Enum.map(points, &elem(&1, 1))
        count = length(values)
        sum = Enum.sum(values)

        %{
          avg: sum / count,
          min: Enum.min(values),
          max: Enum.max(values),
          count: count,
          sum: sum,
          last: values |> List.last()
        }
      end

    {:reply, result, state}
  end

  def handle_call({:enforce_retention, cutoff}, _from_pid, state) do
    {blocks, dropped} = drop_expired_blocks(state.blocks, cutoff)
    raw = Enum.filter(state.raw_buffer, fn {ts, _} -> ts >= cutoff end)

    new_block_count = :queue.len(blocks)
    new_raw_count = length(raw)
    empty? = new_block_count == 0 and new_raw_count == 0

    state = %{
      state
      | blocks: blocks,
        block_count: new_block_count,
        raw_buffer: raw,
        raw_count: new_raw_count,
        dirty: dropped > 0 or new_raw_count != state.raw_count
    }

    {:reply, {:ok, dropped, empty?}, state}
  end

  @impl true
  def handle_info(:flush_to_disk, state) do
    state = flush_to_disk(state)
    flush_ref = Process.send_after(self(), :flush_to_disk, @flush_interval_ms)
    {:noreply, %{state | flush_ref: flush_ref}}
  end

  def handle_info(:maybe_compress_stale, state) do
    state =
      if state.raw_count > 0 && stale?(state) do
        compress_buffer(state)
      else
        state
      end

    stale_ref = Process.send_after(self(), :maybe_compress_stale, @stale_check_ms)
    {:noreply, %{state | stale_ref: stale_ref}}
  end

  @impl true
  def terminate(_reason, state) do
    flush_to_disk(state)
    :ok
  end

  # --- Internals ---

  defp stale?(state) do
    case state.last_write_at do
      nil -> false
      last -> System.monotonic_time(:millisecond) - last > @stale_check_ms
    end
  end

  defp compress_buffer(%{raw_count: 0} = state), do: state

  defp compress_buffer(state) do
    sorted = Enum.sort_by(state.raw_buffer, &elem(&1, 0))

    case GorillaStream.compress(sorted, compression: state.compression) do
      {:ok, data} ->
        {first_ts, _} = List.first(sorted)
        {last_ts, _} = List.last(sorted)

        block = %{
          start_ts: first_ts,
          end_ts: last_ts,
          point_count: state.raw_count,
          data: data
        }

        blocks = :queue.in(block, state.blocks)
        block_count = state.block_count + 1

        {blocks, block_count} =
          if block_count > state.max_blocks do
            {{:value, _dropped}, remaining} = :queue.out(blocks)
            {remaining, block_count - 1}
          else
            {blocks, block_count}
          end

        %{
          state
          | blocks: blocks,
            block_count: block_count,
            raw_buffer: [],
            raw_count: 0,
            dirty: true
        }

      {:error, _reason} ->
        # Compression failed — keep raw buffer as-is
        state
    end
  end

  defp query_raw_points(state, from, to) do
    # Decompress overlapping blocks
    block_points =
      state.blocks
      |> :queue.to_list()
      |> Enum.filter(fn block -> block.end_ts >= from and block.start_ts <= to end)
      |> Enum.flat_map(fn block ->
        case GorillaStream.decompress(block.data, compression: state.compression) do
          {:ok, points} ->
            Enum.filter(points, fn {ts, _} -> ts >= from and ts <= to end)

          {:error, _} ->
            []
        end
      end)

    # Filter raw buffer points
    raw_points =
      state.raw_buffer
      |> Enum.filter(fn {ts, _} -> ts >= from and ts <= to end)

    # Merge and sort
    (block_points ++ raw_points)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp get_latest(state) do
    # Check raw buffer first (most recent data)
    case state.raw_buffer do
      [] ->
        # Check the last block
        case :queue.peek_r(state.blocks) do
          {:value, block} ->
            case GorillaStream.decompress(block.data, compression: state.compression) do
              {:ok, points} ->
                points |> Enum.max_by(&elem(&1, 0))

              {:error, _} ->
                nil
            end

          :empty ->
            nil
        end

      buffer ->
        Enum.max_by(buffer, &elem(&1, 0))
    end
  end

  defp load_from_disk(state) do
    path = BlockStore.series_path(state.data_dir, state.series_id)

    case BlockStore.read(path) do
      {:ok, {blocks, raw_buffer}} ->
        block_count = :queue.len(blocks)

        %{
          state
          | blocks: blocks,
            block_count: block_count,
            raw_buffer: raw_buffer,
            raw_count: length(raw_buffer)
        }

      {:error, _} ->
        state
    end
  end

  defp drop_expired_blocks(queue, cutoff) do
    drop_expired_blocks(queue, cutoff, 0)
  end

  defp drop_expired_blocks(queue, cutoff, dropped) do
    case :queue.peek(queue) do
      {:value, block} when block.end_ts < cutoff ->
        {{:value, _}, remaining} = :queue.out(queue)
        drop_expired_blocks(remaining, cutoff, dropped + 1)

      _ ->
        {queue, dropped}
    end
  end

  defp flush_to_disk(%{dirty: false} = state), do: state

  defp flush_to_disk(state) do
    path = BlockStore.series_path(state.data_dir, state.series_id)
    BlockStore.write(path, state.blocks, state.raw_buffer)
    %{state | dirty: false}
  end
end
