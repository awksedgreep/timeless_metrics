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
    :compression,
    :compression_level,
    :shard_id,
    :data_dir,
    :name,
    :store,
    :schema,
    :shard_store,
    :segments_cache,
    :memory_only
  ]

  # 4 hours in seconds
  # 24 hours — ALP promotes daily for best compression ratio
  @default_segment_duration 86_400

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
    cache = :persistent_term.get({__MODULE__, builder_name, :segments_cache})
    read_from_cache(cache, series_id, from, to)
  end

  defp read_from_cache(cache, series_id, from, to) do
    # ETS key: {series_id, start_time} — ordered_set gives us range scan
    # Value: {end_time, count, blob}
    start_key = {series_id, from}
    results = collect_cache_range(cache, :ets.next(cache, start_key), series_id, to, [])

    # Also check entries that started before `from` but extend into the range
    pre_results =
      case exact_or_prev_key(cache, start_key) do
        {^series_id, start} = key ->
          case :ets.lookup(cache, key) do
            [{_, {end_time, _count, blob}}] when end_time >= from ->
              [[blob, start, end_time]]

            _ ->
              []
          end

        _ ->
          []
      end

    {:ok, pre_results ++ results}
  end

  defp exact_or_prev_key(cache, start_key) do
    case :ets.lookup(cache, start_key) do
      [{^start_key, _value}] -> start_key
      [] -> :ets.prev(cache, start_key)
    end
  end

  defp collect_cache_range(_cache, :"$end_of_table", _sid, _to, acc), do: Enum.reverse(acc)

  defp collect_cache_range(cache, {sid, start} = key, sid, to, acc) when start <= to do
    case :ets.lookup(cache, key) do
      [{_, {end_time, _count, blob}}] ->
        collect_cache_range(cache, :ets.next(cache, key), sid, to, [[blob, start, end_time] | acc])

      _ ->
        collect_cache_range(cache, :ets.next(cache, key), sid, to, acc)
    end
  end

  defp collect_cache_range(_cache, _key, _sid, _to, acc), do: Enum.reverse(acc)

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
    data_dir = Keyword.get(opts, :data_dir)
    store = Keyword.get(opts, :store)
    memory_only = Keyword.get(opts, :memory_only, false)
    segment_duration = Keyword.get(opts, :segment_duration, @default_segment_duration)

    compression = Keyword.get(opts, :compression, :zstd)
    compression_level = Keyword.get(opts, :compression_level, 9)
    schema = Keyword.get(opts, :schema)

    Process.flag(:trap_exit, true)

    # ETS cache for compressed segments — used for ALL reads in both modes
    cache_table = :"#{name}_segments_cache"

    :ets.new(cache_table, [
      :named_table,
      :ordered_set,
      :public,
      read_concurrency: true
    ])

    :persistent_term.put({__MODULE__, name, :segments_cache}, cache_table)

    # Disk storage — server mode only
    shard_store =
      if memory_only do
        nil
      else
        File.mkdir_p!(data_dir)
        ss = TimelessMetrics.ShardStore.init(data_dir, shard_id, segment_duration, name)

        ss =
          if schema do
            tier_names = Enum.map(schema.tiers, & &1.name)

            ss
            |> then(fn s ->
              Enum.reduce(schema.tiers, s, fn tier, acc ->
                TimelessMetrics.ShardStore.init_tier(acc, tier.name)
              end)
            end)
            |> TimelessMetrics.ShardStore.init_watermarks(tier_names)
          else
            ss
          end

        # Load existing segments from disk into ETS cache on startup
        load_segments_to_cache(ss, cache_table)

        ss
      end

    if shard_store do
      :persistent_term.put({__MODULE__, name, :shard_store}, shard_store)
    end

    state = %__MODULE__{
      segments: %{},
      segment_duration: segment_duration,
      compression: compression,
      compression_level: compression_level,
      shard_id: shard_id,
      data_dir: data_dir,
      name: name,
      store: store,
      schema: schema,
      shard_store: shard_store,
      segments_cache: cache_table,
      memory_only: memory_only
    }

    # Stage 2: drain ETS buffer every 10s → term_to_binary + zstd level 1
    Process.send_after(self(), :drain_buffer, :timer.seconds(10))

    # Stage 3: daily, recompress completed windows from fast zstd → ALP
    Process.send_after(self(), :promote_segments, :timer.hours(24))

    {:ok, state}
  end

  @impl true
  def handle_cast({:ingest, grouped_points}, state) do
    {:noreply, %{state | segments: ingest_into_segments(grouped_points, state)}}
  end

  @impl true
  def handle_call({:ingest, grouped_points}, _from, state) do
    {:reply, :ok, %{state | segments: ingest_into_segments(grouped_points, state)}}
  end

  def handle_call(:flush, _from, state) do
    {completed, pending} = split_completed(state.segments, state.segment_duration)

    pending_segs = Map.values(pending)

    if pending_segs != [] do
      write_segments(pending_segs, state)
    end

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
  def handle_info(:drain_buffer, state) do
    # Stage 2: drain ETS buffer → fast compress (term_to_binary + zstd 1) → WAL + cache
    shard_name = :"#{state.store}_shard_#{state.shard_id}"

    try do
      GenServer.call(shard_name, :flush_sync, 5_000)
    catch
      :exit, _ -> :ok
    end

    # Fast-compress any accumulated in-memory segments
    all_segments = Map.values(state.segments)

    if all_segments != [] do
      write_segments_fast(all_segments, state)
    end

    Process.send_after(self(), :drain_buffer, :timer.seconds(10))
    {:noreply, %{state | segments: %{}}}
  end

  def handle_info(:promote_segments, state) do
    # Stage 3: recompress completed time windows from fast zstd → ALP
    {completed, pending} = split_completed(state.segments, state.segment_duration)

    # Promote fast-compressed segments in completed windows to ALP
    promote_to_alp(state)

    # Seal the completed windows
    if completed != [] do
      write_and_seal(completed, state)
    end

    Process.send_after(self(), :promote_segments, :timer.hours(24))
    {:noreply, %{state | segments: pending}}
  end

  @impl true
  def terminate(_reason, state) do
    all_segments = Map.values(state.segments)

    if all_segments != [] do
      write_segments(all_segments, state)
    end

    # Persist tier ETS indexes and watermarks to disk (server mode only)
    if state.shard_store do
      if state.shard_store.tier_state != %{} do
        TimelessMetrics.ShardStore.persist_tier_indexes(state.shard_store)
        TimelessMetrics.ShardStore.cleanup_tiers(state.shard_store)
      end

      TimelessMetrics.ShardStore.persist_watermarks(state.shard_store)
      TimelessMetrics.ShardStore.cleanup_watermarks(state.shard_store)
      :persistent_term.erase({__MODULE__, state.name, :shard_store})
    end

    :ok
  end

  # --- Internals ---

  defp ingest_into_segments(grouped_points, state) do
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
  end

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

  # Stage 2: fast compression — term_to_binary + zstd level 1, tag 0xFA
  @fast_marker 0xFA

  defp compress_segments_fast(segments) do
    segments
    |> Enum.reject(fn seg -> seg.points == [] end)
    |> Enum.flat_map(fn seg ->
      sorted_points = Enum.sort_by(seg.points, &elem(&1, 0))
      raw = :erlang.term_to_binary(sorted_points)
      compressed = :ezstd.compress(raw, 1)
      blob = <<@fast_marker, compressed::binary>>

      point_count = length(sorted_points)
      {last_ts, _} = List.last(sorted_points)

      :telemetry.execute(
        [:timeless_metrics, :segment, :write],
        %{point_count: point_count, compressed_bytes: byte_size(blob)},
        %{series_id: seg.series_id, stage: :fast}
      )

      [{seg.series_id, seg.start_time, last_ts, point_count, blob}]
    end)
  end

  defp write_segments_fast(segments, state) do
    compressed = compress_segments_fast(segments)

    if compressed != [] do
      write_to_cache(compressed, state.segments_cache)

      if state.shard_store do
        TimelessMetrics.ShardStore.write_wal(state.shard_store, compressed)
      end
    end
  end

  # Stage 3: read fast-compressed segments from cache, decompress, recompress with ALP
  defp promote_to_alp(state) do
    now = System.os_time(:second)
    current_bucket = segment_bucket(now, state.segment_duration)
    cache = state.segments_cache

    # Scan cache for fast-compressed entries (tag 0xFA) in completed windows
    fast_entries =
      :ets.foldl(
        fn
          {{sid, start} = key, {end_time, count, <<@fast_marker, _::binary>> = blob}}, acc ->
            bucket = segment_bucket(start, state.segment_duration)

            if bucket < current_bucket do
              [{key, sid, start, end_time, count, blob} | acc]
            else
              acc
            end

          {{_sid, _start}, _val}, acc ->
            acc
        end,
        [],
        cache
      )

    if fast_entries != [] do
      # Group by series_id, decompress all fast segments, combine points
      by_series =
        fast_entries
        |> Enum.group_by(fn {_key, sid, _start, _end, _count, _blob} -> sid end)

      Enum.each(by_series, fn {series_id, entries} ->
        # Decompress all fast segments for this series and combine points
        all_points =
          entries
          |> Enum.flat_map(fn {_key, _sid, _start, _end, _count,
                               <<@fast_marker, compressed::binary>>} ->
            raw = :ezstd.decompress(compressed)
            :erlang.binary_to_term(raw)
          end)
          |> Enum.sort_by(&elem(&1, 0))

        if all_points != [] do
          {first_ts, _} = hd(all_points)
          {last_ts, _} = List.last(all_points)
          is_text = match?([{_, v} | _] when is_binary(v), all_points)

          result =
            if is_text do
              case TimelessMetrics.TextCodec.compress(all_points) do
                {:ok, blob} -> {:ok, <<0xFE, blob::binary>>}
                error -> error
              end
            else
              case ExAlp.compress(all_points,
                     compression: :zstd,
                     compression_level: state.compression_level
                   ) do
                {:ok, blob} -> {:ok, <<0xA1, blob::binary>>}
                error -> error
              end
            end

          case result do
            {:ok, alp_blob} ->
              point_count = length(all_points)

              :telemetry.execute(
                [:timeless_metrics, :segment, :promote],
                %{point_count: point_count, compressed_bytes: byte_size(alp_blob)},
                %{series_id: series_id, stage: :alp}
              )

              # Delete old fast entries from cache
              Enum.each(entries, fn {key, _, _, _, _, _} -> :ets.delete(cache, key) end)

              # Write new ALP entry to cache
              :ets.insert(cache, {{series_id, first_ts}, {last_ts, point_count, alp_blob}})

              # Update disk storage
              if state.shard_store do
                TimelessMetrics.ShardStore.write_wal(state.shard_store, [
                  {series_id, first_ts, last_ts, point_count, alp_blob}
                ])
              end

            {:error, reason} ->
              require Logger
              Logger.warning("Failed to promote series #{series_id} to ALP: #{inspect(reason)}")
          end
        end
      end)
    end
  end

  # Stage 3 (existing): ALP compression for write_and_seal and explicit flush
  defp compress_segments(segments, state) do
    segments
    |> Enum.reject(fn seg -> seg.points == [] end)
    |> Enum.flat_map(fn seg ->
      sorted_points = Enum.sort_by(seg.points, &elem(&1, 0))

      # Detect text series by checking if the first value is a string
      is_text = match?([{_, v} | _] when is_binary(v), sorted_points)

      result =
        if is_text do
          case TimelessMetrics.TextCodec.compress(sorted_points) do
            {:ok, blob} -> {:ok, <<0xFE, blob::binary>>}
            error -> error
          end
        else
          # ALP encoding with zstd container compression
          case ExAlp.compress(sorted_points,
                 compression: :zstd,
                 compression_level: state.compression_level
               ) do
            {:ok, blob} -> {:ok, <<0xA1, blob::binary>>}
            error -> error
          end
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

  # Write segments: compress, store in ETS cache, optionally persist to disk
  defp write_segments(segments, state) do
    compressed = compress_segments(segments, state)

    if compressed != [] do
      # Always write to ETS cache (read path)
      write_to_cache(compressed, state.segments_cache)

      # Persist to disk in server mode
      if state.shard_store do
        TimelessMetrics.ShardStore.write_wal(state.shard_store, compressed)
      end
    end
  end

  # Write completed segments: compress, cache, and seal to .seg files (server mode)
  defp write_and_seal(segments, state) do
    compressed = compress_segments(segments, state)

    if compressed != [] do
      # Always write to ETS cache (read path)
      write_to_cache(compressed, state.segments_cache)

      # Persist + seal in server mode
      if state.shard_store do
        TimelessMetrics.ShardStore.write_wal(state.shard_store, compressed)

        compressed
        |> Enum.map(fn {_sid, start, _, _, _} ->
          segment_bucket(start, state.segment_duration)
        end)
        |> Enum.uniq()
        |> Enum.each(fn window ->
          TimelessMetrics.ShardStore.seal_window(state.shard_store, window)
        end)
      end
    end
  end

  defp write_to_cache(compressed_entries, cache) do
    Enum.each(compressed_entries, fn {sid, start, _end_time, _count, _blob} = entry ->
      key = {sid, start}

      merged_entry =
        case :ets.lookup(cache, key) do
          [{^key, {existing_end, existing_count, existing_blob}}] ->
            TimelessMetrics.ShardStore.merge_cache_entries(
              {sid, start, existing_end, existing_count, existing_blob},
              entry
            )

          [] ->
            entry
        end

      {_sid, _start, merged_end, merged_count, merged_blob} = merged_entry
      :ets.insert(cache, {key, {merged_end, merged_count, merged_blob}})
    end)
  end

  # Load existing .seg + WAL data into ETS cache on startup (server mode)
  defp load_segments_to_cache(shard_store, cache) do
    case TimelessMetrics.ShardStore.read_all_entries(shard_store) do
      entries when is_list(entries) ->
        rows =
          Enum.map(entries, fn {sid, start, end_time, count, blob} ->
            {{sid, start}, {end_time, count, blob}}
          end)

        :ets.insert(cache, rows)

      _ ->
        :ok
    end
  end
end
