# Bug: chunk index shadowing when two chunks share (series_id, min_ts)

**Status:** fixed 2026-07-22 — index key widened to `(PartitionKey, min_ts, chunk_seq)`
(suggested fix 1); regression test `duplicate_min_ts_chunks_do_not_shadow` added
**Component:** Rust engine, `native/tms_engine/src/lib.rs`
**Severity:** data invisibility (silent), low probability in steady-state
scraping, higher under backfill / duplicate-timestamp ingest
**Found:** 2026-07-22, via a randomized property harness (plain-table oracle)
run against the extracted copy of this engine in the timeless-libsql
experiment (`~/Documents/rust/timeless-libsql`). The affected index code is
line-for-line identical in both copies.

## Summary

The chunk index is a `BTreeMap` keyed by `(PartitionKey, min_ts)`:

- declaration: `lib.rs:452` — `index: RwLock<BTreeMap<(PartitionKey, i64), ChunkMeta>>`
- insert sites: `lib.rs:912` (flush), `1005`, `1049`, `1088` (batched/cold
  flush paths), `2064`, `2071` (restart recovery scan)

`BTreeMap::insert` on an existing key **replaces** the value. If two chunks
for the same series have the same `min_ts`, the second insert silently
shadows the first: the earlier chunk's points become unqueryable even though
its data remains on disk. Recovery (`2064`/`2071`) has the same collision, so
a restart does not repair it — whichever chunk the directory scan visits
last wins.

## How it can happen

1. **Backfill / duplicate timestamps across flush boundaries.** Two flush
   cycles each produce a chunk for series S whose first (earliest) point has
   the same timestamp — e.g. re-ingesting an overlapping export, retried
   batches, or clients that clamp/floor timestamps.
2. **Same-second scrapes with second-resolution timestamps.** Any path where
   a partition is flushed twice while the earliest buffered ts is unchanged.
3. **Compaction landing on an occupied key.** A merged chunk keyed at a
   `min_ts` equal to a remaining chunk's `min_ts` for the same series.

Steady-state scraping with strictly-increasing timestamps never hits this,
which is why it has stayed latent.

## Impact

- Points in the shadowed chunk are invisible to all queries (raw and
  aggregate) but still occupy disk.
- The shadowed chunk's file entry is no longer referenced by the index, so
  retention/compaction bookkeeping can strand or double-count it depending
  on path.
- No error is surfaced anywhere; detection requires comparing ingested
  counts vs queryable counts.

## Reproduction sketch (engine level)

```text
resolve series S
write_point(S, ts=100, v=1.0); flush_all()      # chunk A: min_ts=100
write_point(S, ts=100, v=2.0)                   # duplicate ts (backfill)
write_point(S, ts=200, v=3.0); flush_all()      # chunk B: min_ts=100 → shadows A
query_range(S, 0, 1000)                         # returns B's points only;
                                                # chunk A's points are gone
```

(Confirmed against the extracted engine copy; the oracle harness there now
generates strictly-increasing per-series timestamps to work around exactly
this, with a pointer back to this report.)

## Suggested fixes (pick one)

1. **Widen the key**: `(PartitionKey, min_ts, chunk_seq)` where `chunk_seq`
   is any per-engine monotonic id (the chunk file id already exists and is
   unique). Range scans stay cheap (`range((pk, ts, 0)..)`); collisions
   become impossible. Recovery uses the file id it already knows. This is
   the minimal, mechanical fix.
2. **Multi-value entries**: `BTreeMap<(PartitionKey, i64), SmallVec<ChunkMeta>>`
   — smaller key churn, slightly more invasive at all six call sites.
3. **Detect-and-merge on insert**: if the key exists, merge the two chunks'
   metadata/points. Most work; probably not worth it.

Option 1 is recommended. All six insert sites plus the query-side
`index.range(...)` iteration (`~lib.rs:1700`) and delete/compaction removals
need the widened key; the change is mechanical and the existing test suite
plus one new duplicate-min_ts regression test should cover it.
