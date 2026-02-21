defmodule TimelessMetrics.SegmentBuilder do
  @moduledoc """
  Accumulates points per series and writes gorilla-compressed segments to
  file-based storage (ShardStore).

  Each SegmentBuilder owns its own shard directory. Points arrive in batches
  from the paired Buffer shard. The SegmentBuilder groups them into
  time-bounded segments (default 4h), compresses with GorillaStream + zstd,
  and writes to ShardStore (WAL + immutable .seg files).

  Tier chunks, watermarks, and raw segments are all in ShardStore.
  No SQLite is used for shard data.
  """

  use GenServer

  defstruct [
    :segments,
    :segment_duration,
    :pending_flush_interval,
    :compression,
    :compression_level,
    :shard_id,
    :data_dir,
    :name,
    :store,
    :schema,
    :shard_store
  ]

  # 4 hours in seconds
  @default_segment_duration 14_400
  @default_pending_flush_interval :timer.seconds(60)

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Ingest a batch of grouped points. Called by Buffer shards after flush."
  def ingest(builder, grouped_points) do
    GenServer.cast(builder, {:ingest, grouped_points})
  end

  @doc "Synchronous ingest. Used during shutdown to ensure data is received before termination."
  def ingest_sync(builder, grouped_points) do
    GenServer.call(builder, {:ingest, grouped_points}, :infinity)
  end

  @doc "Force flush all open segments to disk."
  def flush(builder) do
    GenServer.call(builder, :flush, :infinity)
  end

  @doc "Get the count of points held in memory (not yet in a finalized segment)."
  def pending_point_count(builder) do
    GenServer.call(builder, :pending_point_count, :infinity)
  end

  # --- Raw segment APIs (Phase 1: file-based storage) ---

  @doc """
  Read raw segments for a specific series within a time range. Lock-free.

  Returns `{:ok, [[data, start_time, end_time], ...]}` sorted by start_time.
  """
  def read_raw_segments(builder_name, series_id, from, to) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.read_segments(store, series_id, from, to)
  end

  @doc """
  Read raw segments for ALL series within a time range (used by rollup). Lock-free.

  Returns `{:ok, [[series_id, start_time, end_time, data], ...]}` sorted by (series_id, start_time).
  """
  def read_raw_for_rollup(builder_name, from, to) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.read_all_segments(store, from, to)
  end

  @doc """
  Read the latest raw segment for a series. Lock-free.

  Returns `{:ok, [[data]]}` or `{:ok, []}`.
  """
  def read_raw_latest(builder_name, series_id) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.read_latest(store, series_id)
  end

  @doc """
  Delete raw segments with end_time before cutoff. Goes through GenServer.
  """
  def delete_raw_before(builder_name, cutoff) do
    GenServer.call(builder_name, {:delete_raw_before, cutoff}, :infinity)
  end

  @doc """
  Get distinct series_ids present in raw storage. Lock-free.

  Returns `{:ok, [[series_id], ...]}`.
  """
  def raw_series_ids(builder_name) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.distinct_series_ids(store)
  end

  @doc """
  Get aggregate stats for raw segments in this shard. Lock-free.

  Returns `%{segment_count, total_points, raw_bytes, oldest_ts, newest_ts}`.
  """
  def raw_stats(builder_name) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.stats(store)
  end

  # --- Tier chunk APIs (Phase 2: file-based storage) ---

  @doc """
  Read tier chunks for a specific series within a time range. Lock-free.

  Returns `{:ok, [[data], ...]}` sorted by chunk_start.
  """
  def read_tier_chunks(builder_name, tier_name, series_id, from, to) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.read_tier_range(store, tier_name, series_id, from, to)
  end

  @doc """
  Read a single tier chunk by exact key (for rollup merge). Lock-free.

  Returns the blob binary, or nil if not found.
  """
  def read_tier_chunk_for_merge(builder_name, tier_name, series_id, chunk_start) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.read_tier_chunk(store, tier_name, series_id, chunk_start)
  end

  @doc """
  Read the latest tier chunk for a series. Lock-free.

  Returns `{:ok, [[data]]}` or `{:ok, []}`.
  """
  def read_tier_latest(builder_name, tier_name, series_id) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.read_tier_latest(store, tier_name, series_id)
  end

  @doc """
  Read tier chunks for ALL series in a time range (for tier-to-tier rollup). Lock-free.

  Returns `{:ok, [[series_id, data], ...]}`.
  """
  def read_tier_for_rollup(builder_name, tier_name, from, to) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.read_tier_for_rollup(store, tier_name, from, to)
  end

  @doc """
  Get distinct series_ids from a tier. Lock-free.

  Returns `{:ok, [[series_id], ...]}`.
  """
  def read_tier_series_ids(builder_name, tier_name) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.tier_series_ids(store, tier_name)
  end

  @doc """
  Get aggregate stats for a tier. Lock-free.

  Returns `{chunks, buckets, compressed_bytes}`.
  """
  def read_tier_stats(builder_name, tier_name) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.tier_stats(store, tier_name)
  end

  @doc """
  Write a batch of tier chunks. Goes through GenServer for write serialization.

  entries = [{series_id, chunk_start, chunk_end, bucket_count, blob}, ...]
  """
  def write_tier_batch(builder_name, tier_name, entries) do
    GenServer.call(builder_name, {:write_tier_batch, tier_name, entries}, :infinity)
  end

  @doc """
  Delete tier chunks where chunk_end < cutoff. Goes through GenServer.
  """
  def delete_tier_before(builder_name, tier_name, cutoff) do
    GenServer.call(builder_name, {:delete_tier_before, tier_name, cutoff}, :infinity)
  end

  # --- Watermark APIs (Phase 3: binary file + ETS) ---

  @doc """
  Read a watermark value for a tier. Lock-free (ETS lookup).

  Returns the integer watermark value, or 0 if not set.
  """
  def read_watermark(builder_name, tier_name) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.read_watermark(store, tier_name)
  end

  @doc """
  Write a watermark value for a tier. Goes through GenServer.
  """
  def write_watermark(builder_name, tier_name, value) do
    GenServer.call(builder_name, {:write_watermark, tier_name, value}, :infinity)
  end

  # --- Compaction APIs (Phase 4) ---

  @doc """
  Calculate dead bytes in a tier's chunks.dat. Lock-free.

  Returns `{dead_bytes, total_file_bytes}`.
  """
  def tier_dead_bytes(builder_name, tier_name) do
    store = :persistent_term.get({__MODULE__, builder_name, :shard_store})
    TimelessMetrics.ShardStore.tier_dead_bytes(store, tier_name)
  end

  @doc """
  Compact a tier's chunks.dat by rewriting only live entries. Goes through GenServer.

  ## Options

    * `:threshold` - minimum dead/total ratio to trigger (default: 0.3)

  Returns `{:ok, reclaimed_bytes}` or `:noop`.
  """
  def compact_tier(builder_name, tier_name, opts \\ []) do
    GenServer.call(builder_name, {:compact_tier, tier_name, opts}, :infinity)
  end

  # --- Server ---

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_id = Keyword.fetch!(opts, :shard_id)
    data_dir = Keyword.fetch!(opts, :data_dir)
    store = Keyword.get(opts, :store)
    segment_duration = Keyword.get(opts, :segment_duration, @default_segment_duration)

    pending_flush_interval =
      Keyword.get(opts, :pending_flush_interval, @default_pending_flush_interval)

    compression = Keyword.get(opts, :compression, :zstd)
    compression_level = Keyword.get(opts, :compression_level, 9)
    schema = Keyword.get(opts, :schema)

    Process.flag(:trap_exit, true)

    File.mkdir_p!(data_dir)

    # Initialize file-based storage (raw segments + tiers + watermarks)
    shard_store = TimelessMetrics.ShardStore.init(data_dir, shard_id, segment_duration, name)

    shard_store =
      if schema do
        tier_names = Enum.map(schema.tiers, & &1.name)

        shard_store
        |> then(fn ss ->
          Enum.reduce(schema.tiers, ss, fn tier, acc ->
            TimelessMetrics.ShardStore.init_tier(acc, tier.name)
          end)
        end)
        |> TimelessMetrics.ShardStore.init_watermarks(tier_names)
      else
        shard_store
      end

    # Store in persistent_term for lock-free access
    :persistent_term.put({__MODULE__, name, :shard_store}, shard_store)

    state = %__MODULE__{
      segments: %{},
      segment_duration: segment_duration,
      pending_flush_interval: pending_flush_interval,
      compression: compression,
      compression_level: compression_level,
      shard_id: shard_id,
      data_dir: data_dir,
      name: name,
      store: store,
      schema: schema,
      shard_store: shard_store
    }

    # Periodic check for completed segments
    Process.send_after(self(), :check_segments, :timer.seconds(10))

    # Periodic flush of in-progress segments to make fresh data queryable
    Process.send_after(self(), :flush_pending, pending_flush_interval)

    {:ok, state}
  end

  @impl true
  def handle_cast({:ingest, grouped_points}, state) do
    new_segments =
      Enum.reduce(grouped_points, state.segments, fn {series_id, points}, segments ->
        Enum.reduce(points, segments, fn {ts, val}, segs ->
          bucket = segment_bucket(ts, state.segment_duration)
          key = {series_id, bucket}

          seg =
            Map.get(segs, key, %{
              series_id: series_id,
              start_time: bucket,
              end_time: bucket + state.segment_duration,
              points: []
            })

          updated = %{seg | points: [{ts, val} | seg.points]}
          Map.put(segs, key, updated)
        end)
      end)

    # Accumulate only — sealing happens on flush or :check_segments timer.
    # This ensures all points for a series+window are compressed into one
    # gorilla stream, regardless of how many buffer flushes delivered them.
    {:noreply, %{state | segments: new_segments}}
  end

  @impl true
  def handle_call({:ingest, grouped_points}, _from, state) do
    {:noreply, new_state} = handle_cast({:ingest, grouped_points}, state)
    {:reply, :ok, new_state}
  end

  def handle_call(:flush, _from, state) do
    {completed, pending} = split_completed(state.segments, state.segment_duration)

    # Write pending segments to WAL (makes fresh data queryable)
    pending_segs = Map.values(pending)

    if pending_segs != [] do
      write_segments(pending_segs, state)
    end

    # Seal completed windows into .seg files
    if completed != [] do
      write_and_seal(completed, state)
    end

    {:reply, :ok, %{state | segments: pending}}
  end

  def handle_call(:pending_point_count, _from, state) do
    count =
      state.segments
      |> Map.values()
      |> Enum.reduce(0, fn seg, acc -> acc + length(seg.points) end)

    {:reply, count, state}
  end

  def handle_call({:delete_raw_before, cutoff}, _from, state) do
    result = TimelessMetrics.ShardStore.delete_before(state.shard_store, cutoff)
    {:reply, result, state}
  end

  def handle_call({:write_tier_batch, tier_name, entries}, _from, state) do
    TimelessMetrics.ShardStore.write_tier_batch(state.shard_store, tier_name, entries)
    {:reply, :ok, state}
  end

  def handle_call({:delete_tier_before, tier_name, cutoff}, _from, state) do
    TimelessMetrics.ShardStore.delete_tier_before(state.shard_store, tier_name, cutoff)
    {:reply, :ok, state}
  end

  def handle_call({:write_watermark, tier_name, value}, _from, state) do
    TimelessMetrics.ShardStore.write_watermark(state.shard_store, tier_name, value)
    {:reply, :ok, state}
  end

  def handle_call({:compact_tier, tier_name, opts}, _from, state) do
    result = TimelessMetrics.ShardStore.compact_tier(state.shard_store, tier_name, opts)
    {:reply, result, state}
  end

  @impl true
  def handle_info(:check_segments, state) do
    {completed, pending} = split_completed(state.segments, state.segment_duration)

    if completed != [] do
      write_and_seal(completed, state)
    end

    Process.send_after(self(), :check_segments, :timer.seconds(10))
    {:noreply, %{state | segments: pending}}
  end

  def handle_info(:flush_pending, state) do
    # Write in-progress segments to WAL so fresh data is queryable,
    # but keep them in memory for continued accumulation.
    pending = Map.values(state.segments)

    if pending != [] do
      write_segments(pending, state)
    end

    Process.send_after(self(), :flush_pending, state.pending_flush_interval)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    all_segments = Map.values(state.segments)

    if all_segments != [] do
      write_segments(all_segments, state)
    end

    # Persist tier ETS indexes and watermarks to disk
    if state.shard_store.tier_state != %{} do
      TimelessMetrics.ShardStore.persist_tier_indexes(state.shard_store)
      TimelessMetrics.ShardStore.cleanup_tiers(state.shard_store)
    end

    TimelessMetrics.ShardStore.persist_watermarks(state.shard_store)
    TimelessMetrics.ShardStore.cleanup_watermarks(state.shard_store)

    # Clean up persistent_term
    :persistent_term.erase({__MODULE__, state.name, :shard_store})

    :ok
  end

  # --- Internals ---

  defp segment_bucket(timestamp, duration) do
    div(timestamp, duration) * duration
  end

  defp split_completed(segments, duration) do
    now = System.os_time(:second)
    current_bucket = segment_bucket(now, duration)

    {completed_map, pending_map} =
      Enum.split_with(segments, fn {{_series_id, bucket}, _seg} ->
        bucket < current_bucket
      end)

    completed = Enum.map(completed_map, fn {_key, seg} -> seg end)
    pending = Map.new(pending_map)

    {completed, pending}
  end

  # Dict-compressed blob prefix: <<0xFF, dict_version>>
  # Standard blobs start with zstd magic (0x28) or gorilla magic (0x47), never 0xFF
  @dict_marker 0xFF

  defp compress_segments(segments, state) do
    cdict = state.store && TimelessMetrics.DictTrainer.get_cdict(state.store)
    dict_version = state.store && TimelessMetrics.DictTrainer.get_dict_version(state.store)

    Enum.flat_map(segments, fn seg ->
      sorted_points = Enum.sort_by(seg.points, &elem(&1, 0))

      result =
        if cdict && dict_version > 0 do
          compress_with_dict(sorted_points, cdict, dict_version)
        else
          GorillaStream.compress(sorted_points,
            compression: state.compression,
            compression_level: state.compression_level
          )
        end

      case result do
        {:ok, blob} ->
          point_count = length(sorted_points)
          {last_ts, _} = List.last(sorted_points)

          :telemetry.execute(
            [:timeless_metrics, :segment, :write],
            %{point_count: point_count, compressed_bytes: byte_size(blob)},
            %{series_id: seg.series_id}
          )

          [{seg.series_id, seg.start_time, last_ts, point_count, blob}]

        {:error, reason} ->
          require Logger

          Logger.warning(
            "Failed to compress segment for series #{seg.series_id}: #{inspect(reason)}"
          )

          []
      end
    end)
  end

  defp compress_with_dict(sorted_points, cdict, dict_version) do
    # Gorilla compress without container compression, then dict-compress
    with {:ok, gorilla_blob} <- GorillaStream.compress(sorted_points, compression: :none),
         {:ok, compressed} <-
           GorillaStream.Compression.Container.compress_with_dict(gorilla_blob, cdict) do
      {:ok, <<@dict_marker, dict_version::8, compressed::binary>>}
    end
  end

  # Write segments to WAL (for in-progress / pending flush)
  defp write_segments(segments, state) do
    compressed = compress_segments(segments, state)

    if compressed != [] do
      TimelessMetrics.ShardStore.write_wal(state.shard_store, compressed)
    end
  end

  # Write completed segments to WAL, then seal their windows into .seg files
  defp write_and_seal(segments, state) do
    compressed = compress_segments(segments, state)

    if compressed != [] do
      TimelessMetrics.ShardStore.write_wal(state.shard_store, compressed)

      # Seal each completed window
      compressed
      |> Enum.map(fn {_sid, start, _, _, _} -> segment_bucket(start, state.segment_duration) end)
      |> Enum.uniq()
      |> Enum.each(fn window ->
        TimelessMetrics.ShardStore.seal_window(state.shard_store, window)
      end)
    end
  end
end
