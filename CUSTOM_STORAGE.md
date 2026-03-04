# Custom Storage Backend — Design Document

> **Note:** This design document is historical. The custom storage backend described
> here has been implemented as the **per-series actor model with BlockStore v2**
> (`.dat` files). Raw data no longer uses SQLite shard DBs. See
> [docs/architecture.md](docs/architecture.md) for the current architecture.

## Problem

SQLite shard DBs add ~25x storage overhead for write-once time series data.
At 200K series / 7 days: 547 MB actual data → 13.7 GB on disk.

SQLite's WAL, MVCC, page structure, and B-tree overhead exist for
update-heavy transactional workloads. Our data is append-only and immutable
once written. We're paying for features we don't use.

## Goal

Replace SQLite shard DBs with a custom binary format optimized for
write-once time series. Keep SQLite for the main metadata DB (metrics.db).

**Target:** <2 GB total for the same 200K series / 7 days benchmark
(vs 13.7 GB SQLite, vs 547 MB theoretical minimum).

## Architecture

### Current (SQLite shards)

```
data_dir/
  metrics.db              # main DB: series, metadata, annotations, alerts (KEEP)
  shard_0.db              # SQLite: raw_segments + tier tables + watermarks
  shard_1.db
  ...
  shard_N.db
```

### New (custom file storage)

```
data_dir/
  metrics.db              # unchanged — SQLite for metadata
  shard_0/
    raw/
      1706000000.seg      # immutable segment file (one per segment_duration window)
      1706014400.seg
      current.wal         # append log for in-progress window
    tier_hourly/
      chunks.dat          # append-only chunk data file
      index.bin           # sorted (series_id, chunk_start) → (offset, length)
    tier_daily/
      chunks.dat
      index.bin
    tier_monthly/
      chunks.dat
      index.bin
    watermarks.bin         # fixed-size: one int64 per tier
  shard_1/
    ...
```

### Segment File Format (.seg)

Each `.seg` file contains all segments for one time window across all series
assigned to this shard. The file is written once and never modified.

```
┌────────────────────────────────────────┐
│ Magic: "TS" (2 bytes)                  │
│ Version: uint8                         │
│ Segment count: uint32                  │
│ Index offset: uint64                   │
├────────────────────────────────────────┤
│ Segment data (concatenated blobs)      │
│   [gorilla+zstd compressed bytes]      │
│   [gorilla+zstd compressed bytes]      │
│   ...                                  │
├────────────────────────────────────────┤
│ Index (sorted by series_id, start_time)│
│   series_id:  int64                    │
│   start_time: int64                    │
│   end_time:   int64                    │
│   point_count: uint32                  │
│   offset:     uint64                   │
│   length:     uint32                   │
│   (40 bytes per entry)                 │
├────────────────────────────────────────┤
│ Footer: index_offset (uint64)          │
└────────────────────────────────────────┘
```

**Read path:** mmap the file, read footer to find index, binary search index
for (series_id, time range), decompress matching blobs. Zero-copy.

**Write path:** Accumulate segments in memory (same as current SegmentBuilder),
then write sorted file atomically: data → index → footer → fsync → rename.

### Write-Ahead Log (current.wal)

For the in-progress time window, segments are appended to a WAL file
as they arrive. On segment window close:

1. Sort all entries from WAL by (series_id, start_time)
2. Write immutable `.seg` file
3. fsync `.seg`
4. Delete WAL

**Crash recovery:** On startup, if `current.wal` exists, replay it into
memory. If a `.seg` file and WAL overlap, the WAL wins (may contain
updates from pending flushes).

WAL entry format:
```
series_id:   int64
start_time:  int64
end_time:    int64
point_count: uint32
data_length: uint32
data:        [bytes]
```

### Tier Chunk Storage

Same append-only file approach. The `chunks.dat` file has compressed
TierChunk blobs. The `index.bin` maps (series_id, chunk_start) to
file offset.

For rollup read-modify-write:
1. Read existing chunk from chunks.dat via index lookup
2. Merge with new buckets (TierChunk.merge)
3. Append new blob to chunks.dat
4. Update index entry (old blob becomes dead space)
5. Periodic compaction: rewrite chunks.dat without dead space

### Retention

```elixir
# Raw: delete old .seg files
File.rm!(Path.join(shard_dir, "raw/1706000000.seg"))

# Tiers: compaction removes expired chunks
# (or just let dead space accumulate and compact periodically)
```

No DELETE statements, no table scans. O(1) for raw, O(n) for tier compaction
but amortized over periodic runs.

### Watermarks

Fixed-size binary file, memory-mapped:
```
<<hourly_wm::int64, daily_wm::int64, monthly_wm::int64>>
```

24 bytes. Read/write via binary pattern matching. No SQL.

## Implementation Phases

### Phase 1: ShardStore Module + Raw Segment Files

**New file:** `lib/timeless/shard_store.ex`

Replace `SegmentBuilder`'s SQLite raw_segments table with file-based storage.

- `ShardStore.init(shard_dir, schema)` — create directory structure
- `ShardStore.write_segments(shard, segments)` — append to WAL or write .seg
- `ShardStore.read_segments(shard, series_id, from, to)` — mmap + binary search
- `ShardStore.seal_window(shard, window_start)` — WAL → immutable .seg
- Keep SQLite for tier tables initially (swap in Phase 2)

**Modify:** `SegmentBuilder` to use `ShardStore` instead of SQLite for raw data.
The GenServer structure stays the same — ShardStore is the storage backend.

**Tests:** All existing raw segment tests should pass unchanged.

**Benchmark:** Compare storage size and read latency vs SQLite.

### Phase 2: Tier Chunk Files

Replace SQLite tier tables with append-only chunk files + index.

- `ShardStore.write_tier_chunk(shard, tier, series_id, chunk_start, chunk_end, blob)`
- `ShardStore.read_tier_chunk(shard, tier, series_id, chunk_start)` — for rollup merge
- `ShardStore.read_tier_range(shard, tier, series_id, from, to)` — for queries
- Index: sorted list of `{series_id, chunk_start, offset, length}`, binary searchable
- Dead space tracking for compaction

**Modify:** `Rollup` and `Query` to use ShardStore for tier data.

### Phase 3: Watermarks + Retention

- Replace `_watermarks` SQLite table with fixed-size binary file
- Retention: `File.rm!` for raw .seg files, compaction for tier chunks
- Window-based raw retention: delete all .seg files with window_start < cutoff

### Phase 4: Compaction

Periodic rewrite of tier chunk files to reclaim dead space from
read-modify-write updates.

- Triggered when dead_bytes / total_bytes > threshold (e.g., 30%)
- Reads live entries from index, writes new compact file, atomic rename
- Can run in background Task without blocking reads (old file stays mmaped)

### Phase 5: Benchmark + Validate

- Full test suite passes
- Storage comparison at 10K/100K/200K series
- Ingestion rate comparison
- Query latency comparison
- Crash recovery testing (kill mid-write, restart, verify data)

### Phase 6: Parquet Export (Optional/Future)

- Add `parquer` dep (pure Erlang Parquet writer)
- `Timeless.export_parquet(store, tier, opts)` — dump tier data to .parquet
- Useful for shipping to cloud analytics (S3 → Athena/BigQuery)

## Modules Affected

| Module | Change |
|--------|--------|
| `SegmentBuilder` | Replace SQLite with ShardStore calls |
| `ShardStore` (NEW) | File-based storage engine |
| `Rollup` | Use ShardStore for tier read/write |
| `Query` | Use ShardStore for reads |
| `Retention` | File deletion instead of SQL DELETE |
| `Supervisor` | Start ShardStore instead of opening SQLite |
| `Timeless` (info/1) | Report file-based storage stats |

Modules NOT affected: `TierChunk`, `Schema`, `DB`, `SeriesRegistry`,
`Buffer`, `Alert`, `Annotation`, `Chart`, `HTTP`.

## Key Design Decisions

1. **Immutable segment files** — write once, read many, delete whole file.
   No random writes, no fragmentation, no vacuum.

2. **Footer-based index** — index at end of file means we write data first
   (streaming), then build index, then write footer. Single sequential pass.

3. **mmap for reads** — OS page cache handles everything. No buffer pool
   to tune. Reads are zero-copy.

4. **WAL only for current window** — past windows are immutable .seg files.
   WAL is small (one segment_duration worth of data), crash recovery is fast.

5. **Tier compaction vs rewrite** — dead space from read-modify-write
   accumulates slowly (one rollup cycle per chunk per tier). Compaction
   runs infrequently. Acceptable tradeoff for simpler writes.

6. **Keep SQLite for metadata** — metrics.db is small, query-heavy, and
   benefits from SQL. No reason to replace it.

## Risk Assessment

- **Crash safety:** WAL + fsync + atomic rename provides crash recovery
  for the current window. Immutable files can't corrupt. Main risk is
  partial WAL write — handle by detecting truncated entries on replay.

- **Concurrent reads:** mmap provides lock-free concurrent reads.
  Writer only touches WAL and current files. No contention.

- **Complexity:** More code than SQLite, but simpler semantics.
  No SQL parsing, no query planner, no page cache, no WAL checkpoints.

## Expected Results

At 200K series / 7 days (403M points):
- Current SQLite: 13.7 GB
- Custom storage: ~1.5-2 GB (data + indexes + overhead)
- Theoretical minimum: ~600 MB (data only)
- Improvement: **7-9x smaller**

Query latency should be comparable or faster (mmap + binary search
vs SQLite B-tree, both O(log n), but no page overhead for custom).
