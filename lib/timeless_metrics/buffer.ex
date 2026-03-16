defmodule TimelessMetrics.Buffer do
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
    :store,
    :segment_builder,
    :flush_interval,
    :backpressure_threshold,
    :counter,
    :builder_pid
  ]

  @default_flush_interval :timer.seconds(5)
  @default_backpressure_threshold 50_000
  # Minimum points per series before flushing to SegmentBuilder for compression
  @min_points_per_series 32

  # Number of metadata keys stored in ETS (__builder_pid__, __counter__, __threshold__, __bp_state__, __rate_state__)
  @metadata_key_count 3

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
        :ets.insert(
          table,
          {{series_id, timestamp, :erlang.unique_integer([:positive, :monotonic])}, value}
        )

        [{:__counter__, counter}] = :ets.lookup(table, :__counter__)
        :atomics.add(counter, 1, 1)

        :ok

      {:error, :backpressure} = err ->
        :telemetry.execute(
          [:timeless_metrics, :write, :backpressure],
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
            {{sid, ts, :erlang.unique_integer([:positive, :monotonic])}, val}
          end)

        :ets.insert(table, rows)

        [{:__counter__, counter}] = :ets.lookup(table, :__counter__)
        point_count = length(points)
        :atomics.add(counter, 1, point_count)

        :ok

      {:error, :backpressure} = err ->
        :telemetry.execute(
          [:timeless_metrics, :write, :backpressure],
          %{count: 1},
          %{shard: shard_name}
        )

        err
    end
  end

  @doc "Read buffered points for a series within a time range. Lock-free."
  def read_points(shard_name, series_id, from, to) do
    table = table_name(shard_name)

    try do
      # Match: key = {series_id, timestamp, _seq}, guard: ts >= from and ts <= to
      spec = [
        {{{:"$1", :"$2", :_}, :"$3"},
         [{:==, :"$1", series_id}, {:>=, :"$2", from}, {:"=<", :"$2", to}],
         [{{:"$2", :"$3"}}]}
      ]

      :ets.select(table, spec)
    rescue
      _ -> []
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
    store = Keyword.fetch!(opts, :store)
    segment_builder = Keyword.fetch!(opts, :segment_builder)
    flush_interval = Keyword.get(opts, :flush_interval, @default_flush_interval)

    backpressure_threshold =
      Keyword.get(opts, :backpressure_threshold, @default_backpressure_threshold)

    Process.flag(:trap_exit, true)
    table = table_name(name)

    :ets.new(table, [
      :named_table,
      :ordered_set,
      :public,
      write_concurrency: :auto,
      read_concurrency: true
    ])

    # Atomics counter for lock-free point counting (no GenServer cast per write)
    counter = :atomics.new(1, signed: false)

    # Store metadata in ETS for fast access from caller processes
    builder_pid = GenServer.whereis(segment_builder) || segment_builder
    :ets.insert(table, {:__builder_pid__, builder_pid})
    :ets.insert(table, {:__counter__, counter})

    # Backpressure cache: index 1 = 0 (ok) or 1 (backpressure), index 2 = last check monotonic ms
    bp_state = :atomics.new(2, signed: true)
    :ets.insert(table, {:__bp_state__, bp_state})

    schedule_flush(flush_interval)

    state = %__MODULE__{
      table: table,
      shard_id: shard_id,
      store: store,
      segment_builder: segment_builder,
      flush_interval: flush_interval,
      backpressure_threshold: backpressure_threshold,
      counter: counter,
      builder_pid: builder_pid
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:flush_sync, _from, state) do
    do_flush_sync(state)
    {:reply, :ok, state}
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

  defp do_flush(state) do
    # Snapshot a monotonic cutoff — any write with seq > cutoff arrived after
    # this flush started and must NOT be deleted.
    cutoff = :erlang.unique_integer([:positive, :monotonic])

    select_spec = [
      {{{:"$1", :"$2", :"$3"}, :"$4"}, [{:"=<", :"$3", cutoff}], [{{:"$1", :"$2", :"$4"}}]}
    ]

    points = :ets.select(state.table, select_spec)

    if points != [] do
      grouped = Enum.group_by(points, &elem(&1, 0), fn {_, ts, val} -> {ts, val} end)

      # Split: series with enough points for compression vs too few
      {ready, keep} =
        Enum.split_with(grouped, fn {_series_id, pts} ->
          length(pts) >= @min_points_per_series
        end)

      if ready != [] do
        ready_map = Map.new(ready)
        ready_count = Enum.reduce(ready, 0, fn {_, pts}, acc -> acc + length(pts) end)
        ready_series = Map.keys(ready_map) |> MapSet.new()

        # Delete only the flushed points: re-scan for keys belonging to ready series
        # that are at or before the cutoff, then delete them
        delete_keys_spec = [
          {{{:"$1", :"$2", :"$3"}, :_}, [{:"=<", :"$3", cutoff}], [{{:"$1", :"$2", :"$3"}}]}
        ]

        :ets.select(state.table, delete_keys_spec)
        |> Enum.each(fn {sid, ts, seq} ->
          if MapSet.member?(ready_series, sid) do
            :ets.delete(state.table, {sid, ts, seq})
          end
        end)

        :atomics.put(state.counter, 1, max(:ets.info(state.table, :size) - @metadata_key_count, 0))

        :telemetry.execute(
          [:timeless_metrics, :buffer, :flush],
          %{point_count: ready_count, series_count: map_size(ready_map)},
          %{shard: state.shard_id}
        )

        TimelessMetrics.SegmentBuilder.ingest(state.segment_builder, ready_map)
        TimelessMetrics.Stats.add_points_merged(state.store, ready_count)
      end
    end
  end

  # Synchronous flush — flushes ALL points regardless of per-series count.
  # Used by terminate and explicit flush calls.
  defp do_flush_sync(state) do
    cutoff = :erlang.unique_integer([:positive, :monotonic])

    select_spec = [
      {{{:"$1", :"$2", :"$3"}, :"$4"}, [{:"=<", :"$3", cutoff}], [{{:"$1", :"$2", :"$4"}}]}
    ]

    delete_spec = [{{{:_, :_, :"$1"}, :_}, [{:"=<", :"$1", cutoff}], [true]}]

    points = :ets.select(state.table, select_spec)
    :ets.select_delete(state.table, delete_spec)
    :atomics.put(state.counter, 1, max(:ets.info(state.table, :size) - @metadata_key_count, 0))

    if points != [] do
      count = length(points)
      grouped = Enum.group_by(points, &elem(&1, 0), fn {_, ts, val} -> {ts, val} end)
      TimelessMetrics.SegmentBuilder.ingest_sync(state.segment_builder, grouped)
      TimelessMetrics.Stats.add_points_merged(state.store, count)
    end
  end

  @bp_cache_ttl_ms 100

  defp check_backpressure(shard_name) do
    table = table_name(shard_name)

    try do
      bp_state = :ets.lookup_element(table, :__bp_state__, 2)
      now_ms = :erlang.monotonic_time(:millisecond)
      last_check = :atomics.get(bp_state, 2)

      if now_ms - last_check < @bp_cache_ttl_ms do
        # Return cached result
        if :atomics.get(bp_state, 1) == 1, do: {:error, :backpressure}, else: :ok
      else
        # Stale cache — do the real check
        builder_pid = :ets.lookup_element(table, :__builder_pid__, 2)

        result =
          case Process.info(builder_pid, :message_queue_len) do
            {:message_queue_len, len} when len > @default_backpressure_threshold ->
              :atomics.put(bp_state, 1, 1)
              {:error, :backpressure}

            _ ->
              :atomics.put(bp_state, 1, 0)
              :ok
          end

        :atomics.put(bp_state, 2, now_ms)
        result
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
