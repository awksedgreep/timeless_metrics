defmodule Timeless.Buffer do
  @moduledoc """
  Sharded ETS write buffer.

  Incoming metrics land in one of N ETS shards (selected by series_id).
  Each shard flushes periodically or when a point threshold is exceeded,
  handing accumulated points to the SegmentBuilder for compression.

  Supports backpressure: when the SegmentBuilder mailbox exceeds a
  configurable threshold, writes return `{:error, :backpressure}`.
  """

  use GenServer

  defstruct [
    :table,
    :shard_id,
    :segment_builder,
    :flush_interval,
    :flush_threshold,
    :backpressure_threshold,
    :point_count
  ]

  @default_flush_interval :timer.seconds(5)
  @default_flush_threshold 10_000
  @default_backpressure_threshold 50_000

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Write a point to the appropriate shard's ETS table. Lock-free.

  Returns `:ok` on success or `{:error, :backpressure}` if the
  SegmentBuilder is overwhelmed.
  """
  def write(shard_name, series_id, timestamp, value) do
    table = table_name(shard_name)

    # Check backpressure before writing
    case check_backpressure(shard_name) do
      :ok ->
        :ets.insert(table, {{series_id, timestamp, :erlang.unique_integer()}, value})
        GenServer.cast(shard_name, :maybe_flush)
        :ok

      {:error, :backpressure} = err ->
        :telemetry.execute(
          [:timeless, :write, :backpressure],
          %{count: 1},
          %{shard: shard_name}
        )

        err
    end
  end

  @doc "Get the current point count in this shard's buffer."
  def buffer_size(shard_name) do
    table = table_name(shard_name)

    try do
      :ets.info(table, :size)
    rescue
      _ -> 0
    end
  end

  # --- Server ---

  @impl true
  def init(opts) do
    shard_id = Keyword.fetch!(opts, :shard_id)
    name = Keyword.fetch!(opts, :name)
    segment_builder = Keyword.fetch!(opts, :segment_builder)
    flush_interval = Keyword.get(opts, :flush_interval, @default_flush_interval)
    flush_threshold = Keyword.get(opts, :flush_threshold, @default_flush_threshold)

    backpressure_threshold =
      Keyword.get(opts, :backpressure_threshold, @default_backpressure_threshold)

    Process.flag(:trap_exit, true)
    table = table_name(name)

    :ets.new(table, [
      :named_table,
      :ordered_set,
      :public,
      write_concurrency: true
    ])

    # Store builder pid for fast backpressure checks (no GenServer call needed)
    builder_pid = GenServer.whereis(segment_builder) || segment_builder
    :ets.insert(table, {:__builder_pid__, builder_pid})

    schedule_flush(flush_interval)

    state = %__MODULE__{
      table: table,
      shard_id: shard_id,
      segment_builder: segment_builder,
      flush_interval: flush_interval,
      flush_threshold: flush_threshold,
      backpressure_threshold: backpressure_threshold,
      point_count: 0
    }

    {:ok, state}
  end

  @impl true
  def handle_cast(:maybe_flush, state) do
    count = state.point_count + 1

    if count >= state.flush_threshold do
      do_flush(state)
      {:noreply, %{state | point_count: 0}}
    else
      {:noreply, %{state | point_count: count}}
    end
  end

  @impl true
  def handle_info(:flush, state) do
    do_flush(state)
    schedule_flush(state.flush_interval)
    {:noreply, %{state | point_count: 0}}
  end

  @impl true
  def terminate(_reason, state) do
    do_flush_sync(state)
    :ok
  end

  # --- Internals ---

  defp do_flush(state) do
    # Save metadata, flush data, restore metadata
    builder_entry = :ets.lookup(state.table, :__builder_pid__)
    points = :ets.tab2list(state.table)
    :ets.delete_all_objects(state.table)
    Enum.each(builder_entry, &:ets.insert(state.table, &1))

    # Filter out metadata keys
    data_points = Enum.filter(points, fn
      {:__builder_pid__, _} -> false
      _ -> true
    end)

    if data_points != [] do
      grouped =
        data_points
        |> Enum.map(fn {{series_id, timestamp, _uniq}, value} ->
          {series_id, timestamp, value}
        end)
        |> Enum.group_by(&elem(&1, 0), fn {_sid, ts, val} -> {ts, val} end)

      :telemetry.execute(
        [:timeless, :buffer, :flush],
        %{point_count: length(points), series_count: map_size(grouped)},
        %{shard: state.shard_id}
      )

      Timeless.SegmentBuilder.ingest(state.segment_builder, grouped)
    end
  end

  defp do_flush_sync(state) do
    builder_entry = :ets.lookup(state.table, :__builder_pid__)
    points = :ets.tab2list(state.table)
    :ets.delete_all_objects(state.table)
    Enum.each(builder_entry, &:ets.insert(state.table, &1))

    data_points = Enum.filter(points, fn
      {:__builder_pid__, _} -> false
      _ -> true
    end)

    if data_points != [] do
      grouped =
        data_points
        |> Enum.map(fn {{series_id, timestamp, _uniq}, value} ->
          {series_id, timestamp, value}
        end)
        |> Enum.group_by(&elem(&1, 0), fn {_sid, ts, val} -> {ts, val} end)

      Timeless.SegmentBuilder.ingest_sync(state.segment_builder, grouped)
    end
  end

  defp check_backpressure(shard_name) do
    # Look up the builder pid from the shard's ETS metadata
    # (stored during init as a special key)
    table = table_name(shard_name)

    try do
      case :ets.lookup(table, :__builder_pid__) do
        [{:__builder_pid__, builder_pid}] ->
          case Process.info(builder_pid, :message_queue_len) do
            {:message_queue_len, len} when len > @default_backpressure_threshold ->
              {:error, :backpressure}

            _ ->
              :ok
          end

        [] ->
          :ok
      end
    rescue
      _ -> :ok
    end
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush, interval)
  end

  defp table_name(name) when is_atom(name) do
    :"#{name}_buf"
  end
end
