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
    :counter,
    :builder_pid
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

    case check_backpressure(shard_name) do
      :ok ->
        :ets.insert(table, {{series_id, timestamp, :erlang.unique_integer()}, value})
        [{:__counter__, counter}] = :ets.lookup(table, :__counter__)
        count = :atomics.add_get(counter, 1, 1)

        if count >= :ets.lookup_element(table, :__threshold__, 2) do
          GenServer.cast(shard_name, :flush_now)
          :atomics.put(counter, 1, 0)
        end

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

  @doc """
  Bulk-write pre-resolved points to a shard. Called by write_batch.

  `points` is a list of `{series_id, timestamp, value}` tuples.
  Single ETS insert + single atomics update for the whole batch.
  """
  def write_bulk(shard_name, points) do
    table = table_name(shard_name)

    case check_backpressure(shard_name) do
      :ok ->
        rows =
          Enum.map(points, fn {sid, ts, val} ->
            {{sid, ts, :erlang.unique_integer()}, val}
          end)

        :ets.insert(table, rows)

        [{:__counter__, counter}] = :ets.lookup(table, :__counter__)
        count = :atomics.add_get(counter, 1, length(points))

        if count >= :ets.lookup_element(table, :__threshold__, 2) do
          GenServer.cast(shard_name, :flush_now)
          :atomics.put(counter, 1, 0)
        end

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
      [{:__counter__, counter}] = :ets.lookup(table, :__counter__)
      :atomics.get(counter, 1)
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

    # Atomics counter for lock-free point counting (no GenServer cast per write)
    counter = :atomics.new(1, signed: false)

    # Store metadata in ETS for fast access from caller processes
    builder_pid = GenServer.whereis(segment_builder) || segment_builder
    :ets.insert(table, {:__builder_pid__, builder_pid})
    :ets.insert(table, {:__counter__, counter})
    :ets.insert(table, {:__threshold__, flush_threshold})

    schedule_flush(flush_interval)

    state = %__MODULE__{
      table: table,
      shard_id: shard_id,
      segment_builder: segment_builder,
      flush_interval: flush_interval,
      flush_threshold: flush_threshold,
      backpressure_threshold: backpressure_threshold,
      counter: counter,
      builder_pid: builder_pid
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:flush_sync, _from, state) do
    do_flush(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast(:flush_now, state) do
    do_flush(state)
    {:noreply, state}
  end

  # Legacy: support old :maybe_flush casts from single writes
  def handle_cast(:maybe_flush, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(:flush, state) do
    do_flush(state)
    schedule_flush(state.flush_interval)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    do_flush_sync(state)
    :ok
  end

  # --- Internals ---

  # Match spec: select data points as {series_id, timestamp, value}, skip metadata
  @data_match_spec [{{{:"$1", :"$2", :_}, :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}]

  defp do_flush(state) do
    # Select data points in one pass (skips metadata, strips unique ints)
    points = :ets.select(state.table, @data_match_spec)
    # Delete only data rows (3-tuple keys), metadata rows (atom keys) preserved
    :ets.match_delete(state.table, {{:_, :_, :_}, :_})
    # Reset atomics counter
    :atomics.put(state.counter, 1, 0)

    if points != [] do
      grouped = Enum.group_by(points, &elem(&1, 0), fn {_, ts, val} -> {ts, val} end)

      :telemetry.execute(
        [:timeless, :buffer, :flush],
        %{point_count: length(points), series_count: map_size(grouped)},
        %{shard: state.shard_id}
      )

      Timeless.SegmentBuilder.ingest(state.segment_builder, grouped)
    end
  end

  defp do_flush_sync(state) do
    points = :ets.select(state.table, @data_match_spec)
    :ets.match_delete(state.table, {{:_, :_, :_}, :_})
    :atomics.put(state.counter, 1, 0)

    if points != [] do
      grouped = Enum.group_by(points, &elem(&1, 0), fn {_, ts, val} -> {ts, val} end)
      Timeless.SegmentBuilder.ingest_sync(state.segment_builder, grouped)
    end
  end

  defp check_backpressure(shard_name) do
    table = table_name(shard_name)

    try do
      builder_pid = :ets.lookup_element(table, :__builder_pid__, 2)

      case Process.info(builder_pid, :message_queue_len) do
        {:message_queue_len, len} when len > @default_backpressure_threshold ->
          {:error, :backpressure}

        _ ->
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
