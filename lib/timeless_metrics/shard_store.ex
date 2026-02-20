defmodule TimelessMetrics.ShardStore do
  @moduledoc """
  File-based storage backend for raw time series segments and tier chunks.

  Replaces SQLite shard tables with immutable `.seg` files for raw data
  and append-only `chunks.dat` files with ETS indexes for tier data.

  ## File Layout

      shard_N/
        raw/
          1706000000.seg    # immutable segment file per window
          current.wal       # rewritten on each flush with latest in-progress data
        tier_hourly/
          chunks.dat        # append-only chunk data (same entry format as WAL)
          index.ets         # persisted ETS table for fast lookups
        tier_daily/
          chunks.dat
          index.ets

  ## .seg File Format

      ┌─────────────────────────────────────┐
      │ "TS" (2) | version (1) | count (4) │  15 bytes header
      │ index_offset (8)                    │
      ├─────────────────────────────────────┤
      │ segment blobs (concatenated)        │  variable
      ├─────────────────────────────────────┤
      │ index entries (40 bytes each)       │  count * 40
      │   series_id:i64, start:i64, end:i64 │
      │   point_count:u32, offset:u64, len:u32│
      └─────────────────────────────────────┘

  ## WAL / chunks.dat Entry Format

      series_id:i64 | start:i64 | end:i64 | count:u32 | data_len:u32 | data:bytes
  """

  defstruct [
    :raw_dir,
    :shard_id,
    :store_name,
    :segment_duration,
    :tier_state,
    :watermark_table,
    :watermark_path,
    :seg_index_cache
  ]

  @seg_magic "TS"
  @seg_version 1
  @header_size 15
  @index_entry_size 40
  # 8+8+8+4+4
  @entry_header_size 32

  # ========================================================================
  # Raw Segment API
  # ========================================================================

  @doc "Initialize shard storage directory."
  def init(data_dir, shard_id, segment_duration, store_name \\ nil) do
    raw_dir = Path.join(data_dir, "shard_#{shard_id}/raw")
    File.mkdir_p!(raw_dir)
    prefix = store_name || "timeless"
    cache = :ets.new(:"#{prefix}_seg_cache_#{shard_id}", [:set, :public])

    %__MODULE__{
      raw_dir: raw_dir,
      shard_id: shard_id,
      store_name: prefix,
      segment_duration: segment_duration,
      tier_state: %{},
      seg_index_cache: cache
    }
  end

  @doc """
  Write/update in-progress segments to the WAL.

  Merges with existing WAL entries (same series_id+start_time → latest wins).
  Written atomically via tmp file + rename.
  """
  def write_wal(store, entries) do
    wal = wal_path(store)
    existing = read_wal_entries(wal)
    merged = merge_entries(existing, entries)
    write_wal_file(wal, merged)
  end

  @doc """
  Seal a completed time window: move matching WAL entries into an immutable .seg file.
  """
  def seal_window(store, window_start) do
    wal = wal_path(store)
    entries = read_wal_entries(wal)

    {window_entries, remaining} =
      Enum.split_with(entries, fn {_sid, start, _end, _count, _blob} ->
        window_for(start, store.segment_duration) == window_start
      end)

    if window_entries != [] do
      sorted = Enum.sort_by(window_entries, fn {sid, start, _, _, _} -> {sid, start} end)
      seg_path = Path.join(store.raw_dir, "#{window_start}.seg")

      existing_entries = read_seg_entries(store.seg_index_cache, seg_path)
      # Append (not merge) — late arrivals for an already-sealed window must not
      # replace existing entries.  merge_entries replaces by {sid, start} key,
      # which would lose data when start == bucket_start for both old and new.
      final =
        Enum.sort_by(existing_entries ++ sorted, fn {sid, start, _, _, _} -> {sid, start} end)

      write_seg_file(seg_path, final)
      :ets.delete(store.seg_index_cache, seg_path)
    end

    case remaining do
      [] -> File.rm(wal)
      _ -> write_wal_file(wal, remaining)
    end

    :ok
  end

  @doc "Read raw segments for a specific series within a time range."
  def read_segments(store, series_id, from, to) do
    seg_results =
      list_seg_files(store)
      |> Enum.filter(fn {ws, _path} -> window_overlaps?(ws, store.segment_duration, from, to) end)
      |> Enum.flat_map(fn {_ws, path} ->
        read_seg_for_series(store.seg_index_cache, path, series_id, fn start, end_t ->
          end_t >= from and start <= to
        end)
      end)

    wal_results = read_from_wal_filtered(store, &match_series?(&1, series_id, from, to))

    all = merge_read_results(seg_results, wal_results)
    sorted = Enum.sort_by(all, fn {_sid, start, _, _, _} -> start end)
    {:ok, Enum.map(sorted, fn {_sid, start, end_t, _count, blob} -> [blob, start, end_t] end)}
  end

  @doc "Read raw segments for ALL series within a time range (for rollup)."
  def read_all_segments(store, from, to) do
    seg_results =
      list_seg_files(store)
      |> Enum.filter(fn {ws, _path} -> window_overlaps?(ws, store.segment_duration, from, to) end)
      |> Enum.flat_map(fn {_ws, path} ->
        read_seg_matching(store.seg_index_cache, path, fn {_sid, start, end_t, _count, _offset,
                                                           _len} ->
          end_t > from and start < to
        end)
      end)

    wal_results = read_from_wal_filtered(store, &match_time?(&1, from, to))

    all = merge_read_results(seg_results, wal_results)
    sorted = Enum.sort_by(all, fn {sid, start, _, _, _} -> {sid, start} end)
    {:ok, Enum.map(sorted, fn {sid, start, end_t, _count, blob} -> [sid, start, end_t, blob] end)}
  end

  @doc "Read the latest raw segment for a series."
  def read_latest(store, series_id) do
    # Search newest files first for early exit
    seg_results =
      list_seg_files(store)
      |> Enum.reverse()
      |> Enum.reduce_while([], fn {_ws, path}, acc ->
        matches = read_seg_for_series(store.seg_index_cache, path, series_id)

        case matches do
          [] -> {:cont, acc}
          found -> {:halt, found ++ acc}
        end
      end)

    wal_results = read_from_wal_filtered(store, &match_series_any?(&1, series_id))

    all = merge_read_results(seg_results, wal_results)

    case all do
      [] ->
        {:ok, []}

      entries ->
        {_sid, _start, _end, _count, blob} =
          Enum.max_by(entries, fn {_sid, _start, end_t, _count, _blob} -> end_t end)

        {:ok, [[blob]]}
    end
  end

  @doc "Delete raw .seg files and WAL entries before cutoff."
  def delete_before(store, cutoff) do
    list_seg_files(store)
    |> Enum.each(fn {window_start, path} ->
      if window_start + store.segment_duration <= cutoff do
        File.rm(path)
        :ets.delete(store.seg_index_cache, path)
      end
    end)

    wal = wal_path(store)
    entries = read_wal_entries(wal)

    if entries != [] do
      remaining =
        Enum.reject(entries, fn {_sid, _start, end_t, _count, _blob} -> end_t < cutoff end)

      cond do
        remaining == entries -> :ok
        remaining == [] -> File.rm(wal)
        true -> write_wal_file(wal, remaining)
      end
    end

    {:ok, []}
  end

  @doc "Get distinct series_ids across raw .seg files and WAL."
  def distinct_series_ids(store) do
    seg_ids =
      list_seg_files(store)
      |> Enum.flat_map(fn {_window, path} -> read_seg_series_ids(store.seg_index_cache, path) end)

    wal_ids =
      read_wal_entries(wal_path(store))
      |> Enum.map(fn {sid, _, _, _, _} -> sid end)

    all_ids = (seg_ids ++ wal_ids) |> Enum.uniq() |> Enum.sort()
    {:ok, Enum.map(all_ids, fn id -> [id] end)}
  end

  @doc "Get aggregate stats for raw segments."
  def stats(store) do
    seg_entries =
      list_seg_files(store)
      |> Enum.flat_map(fn {_window, path} -> read_seg_stats(store.seg_index_cache, path) end)

    wal_entries =
      read_wal_entries(wal_path(store))
      |> Enum.map(fn {_sid, start, end_t, count, blob} ->
        {start, end_t, count, byte_size(blob)}
      end)

    all = seg_entries ++ wal_entries

    if all == [] do
      %{segment_count: 0, total_points: 0, raw_bytes: 0, oldest_ts: nil, newest_ts: nil}
    else
      %{
        segment_count: length(all),
        total_points: Enum.sum(Enum.map(all, &elem(&1, 2))),
        raw_bytes: Enum.sum(Enum.map(all, &elem(&1, 3))),
        oldest_ts: all |> Enum.map(&elem(&1, 0)) |> Enum.min(),
        newest_ts: all |> Enum.map(&elem(&1, 1)) |> Enum.max()
      }
    end
  end

  # ========================================================================
  # Tier Chunk API
  # ========================================================================

  @doc """
  Initialize tier storage for a specific tier.

  Creates the tier directory, loads or rebuilds the ETS index from chunks.dat,
  and returns the updated store.
  """
  def init_tier(store, tier_name) do
    tier_key = to_string(tier_name)
    shard_dir = Path.dirname(store.raw_dir)
    dir = Path.join(shard_dir, "tier_#{tier_key}")
    File.mkdir_p!(dir)

    chunks_path = Path.join(dir, "chunks.dat")
    index_path = Path.join(dir, "index.ets")
    table_name = :"#{store.store_name}_tier_#{store.shard_id}_#{tier_key}"

    table =
      cond do
        File.exists?(index_path) ->
          case :ets.file2tab(String.to_charlist(index_path)) do
            {:ok, tab} -> tab
            {:error, _} -> rebuild_tier_index(table_name, chunks_path)
          end

        File.exists?(chunks_path) and file_size(chunks_path) > 0 ->
          rebuild_tier_index(table_name, chunks_path)

        true ->
          :ets.new(table_name, [:ordered_set, :public, :named_table])
      end

    tier_info = %{dir: dir, table: table, chunks_path: chunks_path, index_path: index_path}
    %{store | tier_state: Map.put(store.tier_state, tier_name, tier_info)}
  end

  @doc """
  Write a batch of tier chunks. Appends blobs to chunks.dat and updates ETS index.

  entries = [{series_id, chunk_start, chunk_end, bucket_count, blob}, ...]
  """
  def write_tier_batch(store, tier_name, entries) do
    info = store.tier_state[tier_name]
    file_offset = file_size(info.chunks_path)

    # Build iodata for all entries and compute offsets
    {iodata, ets_entries, _} =
      Enum.reduce(entries, {[], [], file_offset}, fn {sid, cs, ce, bc, blob}, {io, idx, pos} ->
        data_len = byte_size(blob)

        header =
          <<sid::signed-64, cs::signed-64, ce::signed-64, bc::unsigned-32, data_len::unsigned-32>>

        data_offset = pos + @entry_header_size
        entry_size = @entry_header_size + data_len

        {[io, header, blob], [{sid, cs, ce, bc, data_offset, data_len} | idx], pos + entry_size}
      end)

    # Append to chunks.dat
    File.write!(info.chunks_path, IO.iodata_to_binary(iodata), [:append, :binary])

    # Update ETS index
    Enum.each(ets_entries, fn {sid, cs, ce, bc, offset, len} ->
      :ets.insert(info.table, {{sid, cs}, ce, bc, offset, len})
    end)
  end

  @doc """
  Read a single tier chunk by exact key (for rollup read-modify-write merge).

  Returns the blob binary, or nil if not found.
  """
  def read_tier_chunk(store, tier_name, series_id, chunk_start) do
    info = store.tier_state[tier_name]

    case :ets.lookup(info.table, {series_id, chunk_start}) do
      [{{^series_id, ^chunk_start}, _ce, _bc, offset, length}] ->
        pread(info.chunks_path, offset, length)

      [] ->
        nil
    end
  end

  @doc """
  Read tier chunks for a specific series within a time range.

  Returns `{:ok, [[data], ...]}` sorted by chunk_start.
  """
  def read_tier_range(store, tier_name, series_id, from, to) do
    info = store.tier_state[tier_name]

    # Match spec: key={$1,$2}, vals={$3,$4,$5,$6}
    # $1=series_id, $2=chunk_start, $3=chunk_end, $4=bucket_count, $5=offset, $6=length
    # Want: $1 == series_id AND $3 > from AND $2 < to
    matches =
      :ets.select(info.table, [
        {{{:"$1", :"$2"}, :"$3", :"$4", :"$5", :"$6"},
         [{:==, :"$1", series_id}, {:>, :"$3", from}, {:<, :"$2", to}], [{{:"$2", :"$5", :"$6"}}]}
      ])

    sorted = Enum.sort_by(matches, &elem(&1, 0))

    rows =
      Enum.map(sorted, fn {_cs, offset, length} ->
        [pread(info.chunks_path, offset, length)]
      end)

    {:ok, rows}
  end

  @doc """
  Read tier chunks for ALL series within a time range (for tier-to-tier rollup).

  Returns `{:ok, [[series_id, data], ...]}` sorted by (series_id, chunk_start).
  """
  def read_tier_for_rollup(store, tier_name, from, to) do
    info = store.tier_state[tier_name]

    matches =
      :ets.select(info.table, [
        {{{:"$1", :"$2"}, :"$3", :"$4", :"$5", :"$6"}, [{:>, :"$3", from}, {:<, :"$2", to}],
         [{{:"$1", :"$2", :"$5", :"$6"}}]}
      ])

    sorted = Enum.sort_by(matches, fn {sid, cs, _, _} -> {sid, cs} end)

    rows =
      Enum.map(sorted, fn {sid, _cs, offset, length} ->
        [sid, pread(info.chunks_path, offset, length)]
      end)

    {:ok, rows}
  end

  @doc """
  Read the latest tier chunk for a series.

  Returns `{:ok, [[data]]}` or `{:ok, []}`.
  """
  def read_tier_latest(store, tier_name, series_id) do
    info = store.tier_state[tier_name]

    matches =
      :ets.select(info.table, [
        {{{:"$1", :"$2"}, :"$3", :"$4", :"$5", :"$6"}, [{:==, :"$1", series_id}],
         [{{:"$2", :"$5", :"$6"}}]}
      ])

    case matches do
      [] ->
        {:ok, []}

      entries ->
        {_cs, offset, length} = Enum.max_by(entries, &elem(&1, 0))
        {:ok, [[pread(info.chunks_path, offset, length)]]}
    end
  end

  @doc """
  Delete tier chunks where chunk_end < cutoff. Removes from ETS index (dead space in chunks.dat).
  """
  def delete_tier_before(store, tier_name, cutoff) do
    info = store.tier_state[tier_name]

    to_delete =
      :ets.select(info.table, [
        {{{:"$1", :"$2"}, :"$3", :"$4", :"$5", :"$6"}, [{:<, :"$3", cutoff}], [{{:"$1", :"$2"}}]}
      ])

    Enum.each(to_delete, fn key -> :ets.delete(info.table, key) end)
    {:ok, []}
  end

  @doc "Get aggregate stats for a tier: {chunks, buckets, compressed_bytes}."
  def tier_stats(store, tier_name) do
    info = store.tier_state[tier_name]

    :ets.foldl(
      fn {{_sid, _cs}, _ce, bc, _offset, len}, {ch, bk, by} ->
        {ch + 1, bk + bc, by + len}
      end,
      {0, 0, 0},
      info.table
    )
  end

  @doc "Get distinct series_ids from a tier."
  def tier_series_ids(store, tier_name) do
    info = store.tier_state[tier_name]

    ids =
      :ets.select(info.table, [
        {{{:"$1", :_}, :_, :_, :_, :_}, [], [:"$1"]}
      ])
      |> Enum.uniq()
      |> Enum.sort()

    {:ok, Enum.map(ids, fn id -> [id] end)}
  end

  @doc """
  Calculate dead bytes in a tier's chunks.dat file.

  Dead bytes are bytes occupied by overwritten entries (from read-modify-write
  during rollup) that are no longer referenced by the ETS index.

  Returns `{dead_bytes, total_file_bytes}`.
  """
  def tier_dead_bytes(store, tier_name) do
    info = store.tier_state[tier_name]
    total_size = file_size(info.chunks_path)

    live_bytes =
      :ets.foldl(
        fn {{_sid, _cs}, _ce, _bc, _offset, len}, acc ->
          acc + @entry_header_size + len
        end,
        0,
        info.table
      )

    {max(total_size - live_bytes, 0), total_size}
  end

  @doc """
  Compact a tier's chunks.dat by rewriting only live entries.

  Reads live entries from ETS index, writes a new compact file,
  atomically renames it over the old file, then updates ETS offsets.

  ## Options

    * `:threshold` - minimum dead_bytes/total_bytes ratio to trigger (default: 0.3)

  Returns `{:ok, reclaimed_bytes}` or `:noop` if below threshold or empty.
  """
  def compact_tier(store, tier_name, opts \\ []) do
    threshold = Keyword.get(opts, :threshold, 0.3)
    info = store.tier_state[tier_name]
    {dead, total} = tier_dead_bytes(store, tier_name)

    cond do
      total == 0 ->
        :noop

      dead / total < threshold ->
        :noop

      true ->
        do_compact_tier(info)
        {:ok, dead}
    end
  end

  defp do_compact_tier(info) do
    # Read all live entries from ETS, sorted by key for locality
    entries =
      :ets.tab2list(info.table)
      |> Enum.sort_by(fn {{sid, cs}, _, _, _, _} -> {sid, cs} end)

    compact_path = info.chunks_path <> ".compact"

    # Build new compact file: read blob from old file, write to new
    {iodata, new_ets_entries, _} =
      Enum.reduce(entries, {[], [], 0}, fn {{sid, cs}, ce, bc, offset, len}, {io, idx, pos} ->
        blob = pread(info.chunks_path, offset, len)

        header =
          <<sid::signed-64, cs::signed-64, ce::signed-64, bc::unsigned-32, len::unsigned-32>>

        new_offset = pos + @entry_header_size
        entry_size = @entry_header_size + len
        {[io, header, blob], [{{sid, cs}, ce, bc, new_offset, len} | idx], pos + entry_size}
      end)

    File.write!(compact_path, IO.iodata_to_binary(iodata), [:binary])

    # Atomic rename: replaces old chunks.dat
    File.rename!(compact_path, info.chunks_path)

    # Update ETS entries with new offsets
    Enum.each(new_ets_entries, fn entry ->
      :ets.insert(info.table, entry)
    end)
  end

  @doc "Persist all tier ETS indexes to disk."
  def persist_tier_indexes(store) do
    Enum.each(store.tier_state, fn {_name, info} ->
      :ets.tab2file(info.table, String.to_charlist(info.index_path))
    end)
  end

  @doc "Delete all tier ETS tables (for cleanup on shutdown)."
  def cleanup_tiers(store) do
    Enum.each(store.tier_state, fn {_name, info} ->
      :ets.delete(info.table)
    end)

    if store.seg_index_cache do
      :ets.delete(store.seg_index_cache)
    end
  end

  # ========================================================================
  # Watermark API
  # ========================================================================

  @doc """
  Initialize watermark storage for this shard.

  Creates an ETS table for lock-free reads and loads persisted values
  from `watermarks.bin`. Missing tiers default to 0.
  """
  def init_watermarks(store, tier_names) do
    shard_dir = Path.dirname(store.raw_dir)
    path = Path.join(shard_dir, "watermarks.bin")
    table_name = :"#{store.store_name}_watermarks_#{store.shard_id}"

    table = :ets.new(table_name, [:set, :public, :named_table])

    # Load from file if exists
    existing = read_watermark_file(path)

    Enum.each(tier_names, fn name ->
      key = to_string(name)
      value = Map.get(existing, key, 0)
      :ets.insert(table, {key, value})
    end)

    %{store | watermark_table: table, watermark_path: path}
  end

  @doc "Read a watermark value. Lock-free (ETS lookup)."
  def read_watermark(store, tier_name) do
    key = to_string(tier_name)

    case :ets.lookup(store.watermark_table, key) do
      [{^key, value}] -> value
      [] -> 0
    end
  end

  @doc "Write a watermark value. Updates ETS and persists to file."
  def write_watermark(store, tier_name, value) do
    key = to_string(tier_name)
    :ets.insert(store.watermark_table, {key, value})
    persist_watermarks(store)
  end

  @doc "Persist all watermarks to binary file."
  def persist_watermarks(%{watermark_table: nil}), do: :ok

  def persist_watermarks(store) do
    entries = :ets.tab2list(store.watermark_table)
    data = encode_watermarks(entries)
    tmp = store.watermark_path <> ".tmp"
    File.write!(tmp, data, [:binary])
    File.rename!(tmp, store.watermark_path)
  end

  @doc "Delete the watermark ETS table (for cleanup on shutdown)."
  def cleanup_watermarks(%{watermark_table: nil}), do: :ok

  def cleanup_watermarks(store) do
    :ets.delete(store.watermark_table)
  end

  # --- Watermark binary codec ---

  defp read_watermark_file(path) do
    case File.read(path) do
      {:ok, <<count::unsigned-32, rest::binary>>} ->
        decode_watermark_entries(rest, count, %{})

      _ ->
        %{}
    end
  end

  defp decode_watermark_entries(_bin, 0, acc), do: acc

  defp decode_watermark_entries(
         <<name_len::unsigned-16, name::binary-size(name_len), watermark::signed-64,
           rest::binary>>,
         remaining,
         acc
       ) do
    decode_watermark_entries(rest, remaining - 1, Map.put(acc, name, watermark))
  end

  defp decode_watermark_entries(_truncated, _remaining, acc), do: acc

  defp encode_watermarks(entries) do
    count = length(entries)

    body =
      Enum.map(entries, fn {name, value} ->
        name_bin = to_string(name)
        name_len = byte_size(name_bin)
        <<name_len::unsigned-16, name_bin::binary, value::signed-64>>
      end)

    [<<count::unsigned-32>> | body] |> IO.iodata_to_binary()
  end

  # ========================================================================
  # Tier Index Rebuild (crash recovery)
  # ========================================================================

  defp rebuild_tier_index(table_name, chunks_path) do
    table = :ets.new(table_name, [:ordered_set, :public, :named_table])

    case File.read(chunks_path) do
      {:ok, data} -> scan_tier_entries(data, 0, table)
      {:error, :enoent} -> :ok
    end

    table
  end

  defp scan_tier_entries(<<>>, _offset, _table), do: :ok

  defp scan_tier_entries(
         <<sid::signed-64, cs::signed-64, ce::signed-64, bc::unsigned-32, data_len::unsigned-32,
           _data::binary-size(data_len), rest::binary>>,
         offset,
         table
       ) do
    data_offset = offset + @entry_header_size
    # Latest entry for same key wins (overwrites in ordered_set)
    :ets.insert(table, {{sid, cs}, ce, bc, data_offset, data_len})
    scan_tier_entries(rest, offset + @entry_header_size + data_len, table)
  end

  # Truncated entry — stop scanning
  defp scan_tier_entries(_truncated, _offset, _table), do: :ok

  # ========================================================================
  # Internal: file helpers
  # ========================================================================

  defp pread(path, offset, length) do
    {:ok, fd} = :file.open(String.to_charlist(path), [:read, :binary, :raw])

    try do
      case :file.pread(fd, offset, length) do
        {:ok, data} -> data
        :eof -> nil
      end
    after
      :file.close(fd)
    end
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{size: s}} -> s
      _ -> 0
    end
  end

  # ========================================================================
  # Internal: raw segment helpers
  # ========================================================================

  defp wal_path(store), do: Path.join(store.raw_dir, "current.wal")

  defp read_wal_entries(path) when is_binary(path) do
    case File.read(path) do
      {:ok, data} -> decode_entries(data)
      {:error, :enoent} -> []
    end
  end

  # Shared entry codec (used by WAL, chunks.dat, and index rebuild)
  defp decode_entries(<<>>), do: []

  defp decode_entries(<<
         sid::signed-64,
         start::signed-64,
         end_t::signed-64,
         count::unsigned-32,
         data_len::unsigned-32,
         data::binary-size(data_len),
         rest::binary
       >>) do
    [{sid, start, end_t, count, data} | decode_entries(rest)]
  end

  defp decode_entries(_truncated), do: []

  defp encode_entry({sid, start, end_t, count, data}) do
    data_len = byte_size(data)

    <<sid::signed-64, start::signed-64, end_t::signed-64, count::unsigned-32,
      data_len::unsigned-32, data::binary>>
  end

  defp write_wal_file(wal_path, entries) do
    tmp = wal_path <> ".tmp"
    data = Enum.map(entries, &encode_entry/1) |> IO.iodata_to_binary()
    File.write!(tmp, data, [:binary])
    File.rename!(tmp, wal_path)
  end

  # --- .seg File Read/Write ---

  defp write_seg_file(path, sorted_entries) do
    {blobs, index_entries, data_size} =
      Enum.reduce(sorted_entries, {[], [], 0}, fn {sid, start, end_t, count, blob},
                                                  {bs, idx, offset} ->
        abs_offset = @header_size + offset
        len = byte_size(blob)
        entry = {sid, start, end_t, count, abs_offset, len}
        {[blob | bs], [entry | idx], offset + len}
      end)

    blobs = Enum.reverse(blobs)
    index_entries = Enum.reverse(index_entries)
    entry_count = length(sorted_entries)
    index_offset = @header_size + data_size

    header =
      <<@seg_magic::binary, @seg_version::8, entry_count::unsigned-32, index_offset::unsigned-64>>

    index_bin =
      Enum.map(index_entries, fn {sid, start, end_t, count, offset, len} ->
        <<sid::signed-64, start::signed-64, end_t::signed-64, count::unsigned-32,
          offset::unsigned-64, len::unsigned-32>>
      end)

    tmp = path <> ".tmp"
    File.write!(tmp, [header | blobs] ++ index_bin, [:binary])
    File.rename!(tmp, path)
  end

  # Opens a .seg file, gets index binary (from cache or pread), calls fun.(fd, index_bin).
  # Returns [] on any error (missing file, bad header, etc).
  defp with_seg_index(cache, path, fun) do
    case :file.open(String.to_charlist(path), [:read, :binary, :raw]) do
      {:ok, fd} ->
        try do
          case get_or_load_index(cache, path, fd) do
            {:ok, index_bin} -> fun.(fd, index_bin)
            :error -> []
          end
        after
          :file.close(fd)
        end

      {:error, _} ->
        []
    end
  end

  defp get_or_load_index(cache, path, fd) do
    case :ets.lookup(cache, path) do
      [{^path, index_bin}] ->
        {:ok, index_bin}

      [] ->
        case :file.pread(fd, 0, @header_size) do
          {:ok, <<@seg_magic, @seg_version::8, count::unsigned-32, index_offset::unsigned-64>>}
          when count > 0 ->
            case :file.pread(fd, index_offset, count * @index_entry_size) do
              {:ok, index_bin} ->
                :ets.insert(cache, {path, index_bin})
                {:ok, index_bin}

              _ ->
                :error
            end

          _ ->
            :error
        end
    end
  end

  # Linear scan: preads index, filters all entries, batch-preads matching blobs.
  # Used by read_all_segments (needs all series in time range).
  defp read_seg_matching(cache, path, filter_fn) do
    with_seg_index(cache, path, fn fd, index_bin ->
      entries = parse_index_entries(index_bin, [])
      matching = Enum.filter(entries, filter_fn)

      case matching do
        [] ->
          []

        _ ->
          locations = Enum.map(matching, fn {_, _, _, _, offset, len} -> {offset, len} end)
          {:ok, blobs} = :file.pread(fd, locations)

          Enum.zip_with(matching, blobs, fn {sid, start, end_t, count, _, _}, blob ->
            {sid, start, end_t, count, blob}
          end)
      end
    end)
  end

  # Binary search: finds entries for a specific series_id via binary search on sorted index.
  # O(log n + k) where k is matching entries, vs O(n) for linear scan.
  defp read_seg_for_series(cache, path, series_id, time_filter_fn \\ fn _s, _e -> true end) do
    with_seg_index(cache, path, fn fd, index_bin ->
      matching = bsearch_collect(index_bin, series_id, time_filter_fn)

      case matching do
        [] ->
          []

        _ ->
          locations = Enum.map(matching, fn {_, _, _, _, offset, len} -> {offset, len} end)
          {:ok, blobs} = :file.pread(fd, locations)

          Enum.zip_with(matching, blobs, fn {sid, start, end_t, count, _, _}, blob ->
            {sid, start, end_t, count, blob}
          end)
      end
    end)
  end

  # Binary search for first entry with sid >= target, then scan forward collecting matches.
  defp bsearch_collect(index_bin, target_sid, time_filter_fn) do
    count = div(byte_size(index_bin), @index_entry_size)
    if count == 0, do: [], else: do_bsearch_collect(index_bin, target_sid, time_filter_fn, count)
  end

  defp do_bsearch_collect(index_bin, target_sid, time_filter_fn, count) do
    first = lower_bound(index_bin, target_sid, 0, count)
    collect_series(index_bin, target_sid, time_filter_fn, first, count, [])
  end

  # Find leftmost index where sid >= target_sid
  defp lower_bound(_bin, _target, lo, hi) when lo >= hi, do: lo

  defp lower_bound(bin, target, lo, hi) do
    mid = div(lo + hi, 2)
    <<sid::signed-64>> = binary_part(bin, mid * @index_entry_size, 8)

    if sid < target do
      lower_bound(bin, target, mid + 1, hi)
    else
      lower_bound(bin, target, lo, mid)
    end
  end

  # Scan forward from lower_bound collecting entries where sid == target_sid
  defp collect_series(_bin, _target, _filter, idx, count, acc) when idx >= count do
    :lists.reverse(acc)
  end

  defp collect_series(bin, target_sid, time_filter_fn, idx, count, acc) do
    offset = idx * @index_entry_size

    <<sid::signed-64, start::signed-64, end_t::signed-64, cnt::unsigned-32,
      data_offset::unsigned-64, len::unsigned-32>> =
      binary_part(bin, offset, @index_entry_size)

    cond do
      sid > target_sid ->
        :lists.reverse(acc)

      sid == target_sid and time_filter_fn.(start, end_t) ->
        entry = {sid, start, end_t, cnt, data_offset, len}
        collect_series(bin, target_sid, time_filter_fn, idx + 1, count, [entry | acc])

      true ->
        collect_series(bin, target_sid, time_filter_fn, idx + 1, count, acc)
    end
  end

  # Tail-recursive parser for binary index → [{sid, start, end_t, count, offset, len}]
  defp parse_index_entries(<<>>, acc), do: :lists.reverse(acc)

  defp parse_index_entries(
         <<sid::signed-64, start::signed-64, end_t::signed-64, count::unsigned-32,
           offset::unsigned-64, len::unsigned-32, rest::binary>>,
         acc
       ) do
    parse_index_entries(rest, [{sid, start, end_t, count, offset, len} | acc])
  end

  defp parse_index_entries(_truncated, acc), do: :lists.reverse(acc)

  # Read all entries from a .seg file (used by seal_window for merge)
  defp read_seg_entries(cache, path) do
    with_seg_index(cache, path, fn fd, index_bin ->
      entries = parse_index_entries(index_bin, [])

      case entries do
        [] ->
          []

        _ ->
          locations = Enum.map(entries, fn {_, _, _, _, offset, len} -> {offset, len} end)
          {:ok, blobs} = :file.pread(fd, locations)

          Enum.zip_with(entries, blobs, fn {sid, start, end_t, count, _, _}, blob ->
            {sid, start, end_t, count, blob}
          end)
      end
    end)
  end

  # Extract just series_ids from index (no blob reads needed)
  defp read_seg_series_ids(cache, path) do
    with_seg_index(cache, path, fn _fd, index_bin ->
      extract_series_ids(index_bin)
    end)
  end

  defp extract_series_ids(<<>>), do: []

  defp extract_series_ids(<<sid::signed-64, _rest_entry::binary-size(32), rest::binary>>) do
    [sid | extract_series_ids(rest)]
  end

  # Extract stats from index (no blob reads needed)
  defp read_seg_stats(cache, path) do
    with_seg_index(cache, path, fn _fd, index_bin ->
      extract_stats(index_bin)
    end)
  end

  defp extract_stats(<<>>), do: []

  defp extract_stats(
         <<_sid::signed-64, start::signed-64, end_t::signed-64, count::unsigned-32,
           _offset::unsigned-64, len::unsigned-32, rest::binary>>
       ) do
    [{start, end_t, count, len} | extract_stats(rest)]
  end

  # --- Filtering helpers ---

  defp window_overlaps?(window_start, segment_duration, from, to) do
    window_start < to and window_start + segment_duration > from
  end

  defp match_series?({sid, start, end_t, _count, _blob}, series_id, from, to) do
    sid == series_id and end_t >= from and start <= to
  end

  defp match_series_any?({sid, _start, _end, _count, _blob}, series_id) do
    sid == series_id
  end

  defp match_time?({_sid, start, end_t, _count, _blob}, from, to) do
    end_t > from and start < to
  end

  defp read_from_wal_filtered(store, filter_fn) do
    read_wal_entries(wal_path(store))
    |> Enum.filter(filter_fn)
  end

  defp list_seg_files(store) do
    case File.ls(store.raw_dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".seg"))
        |> Enum.map(fn name ->
          window_start = name |> String.trim_trailing(".seg") |> String.to_integer()
          {window_start, Path.join(store.raw_dir, name)}
        end)
        |> Enum.sort_by(&elem(&1, 0))

      {:error, :enoent} ->
        []
    end
  end

  defp merge_entries(existing, new) do
    new_keys = MapSet.new(new, fn {sid, start, _, _, _} -> {sid, start} end)

    filtered =
      Enum.reject(existing, fn {sid, start, _, _, _} ->
        MapSet.member?(new_keys, {sid, start})
      end)

    filtered ++ new
  end

  defp merge_read_results(seg_entries, wal_entries) do
    merge_entries(seg_entries, wal_entries)
  end

  defp window_for(timestamp, duration) do
    div(timestamp, duration) * duration
  end
end
