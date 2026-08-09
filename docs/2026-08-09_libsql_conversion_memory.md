# rust_engine → libSQL Conversion Holds Whole Series In Memory

## Summary

`TimelessMetrics.LibsqlMigration` materialises an entire series as an Elixir
list, several times over, and peak memory scales with the **largest single
series** rather than with any batch size. On a 3.9 GB host this OOM-killed the
BEAM mid-migration and left an unresumable staged store.

The paging primitive needed to fix it already exists in the NIF and is unused
by this code path.

Found on `timelessmetrics.com` (2026-08-09). The conversion completed only
after adding 8 GB of swap, peaking near 3.8 GB RSS for a 53.4M-point store.

## What Happened In Production

```text
20:01  conversion starts
20:04  beam.smp OOM-killed at 3,774,600 kB anon-rss
20:04  restart resumes, finds the staged migration unusable, raises:
       "staged migration is incomplete or invalid: {:ok, []}"
```

After adding swap, the same conversion succeeded:

```text
timeless_metrics: rust_engine/ conversion complete
  (%{points: 53387021, series: 377})
```

Roughly four minutes, 53.4M points, 377 series.

## Where The Memory Goes

All references are `lib/timeless_metrics/libsql_migration.ex`.

### 1. Unbounded per-series query (`source_points/3:208`)

```elixir
Nif.engine_query_range(source, metric, labels,
  -9_223_372_036_854_775_808, 9_223_372_036_854_775_807)
```

The full int64 range in one call. Every point of the series lands in one list,
then `Enum.flat_map/2` and `Enum.sort_by/2` each build another.

`Enum.sort_by` is the expensive one: the key is
`fn {ts, value} -> {ts, float_bits(value)} end`, and `float_bits/1` allocates an
8-byte binary **per point** purely to make the sort bit-exact.

### 2. Chunking that does not bound memory (`:171-176`)

```elixir
points
|> Enum.chunk_every(100_000)
|> Enum.each(fn chunk -> ... write_batch ... end)
```

The list is already fully resident before the first chunk is produced. This
bounds the write batch size — a real and separate concern — but does nothing
for peak memory.

### 3. A count that pins the list (`:178`)

```elixir
{series_count + 1, point_count + length(points)}
```

`length(points)` after the write loop keeps the whole list reachable for the
loop's entire duration, so nothing can be incrementally collected.

### 4. Verification, the actual peak (`verify_all/2:225`)

```elixir
expected = source_points(source, metric, labels)          # full source series
{:ok, actual} = LibsqlEngine.query_raw(...)               # full target series
actual = Enum.sort_by(actual, ...)                        # + sort intermediates
...
Enum.zip(left, right) |> Enum.all?(...)                   # + a third list
```

Source and target for the same series are alive simultaneously, each with sort
intermediates, and `bit_exact?/2:246` then builds a zipped list of 2-tuples on
top. This is roughly four to five concurrent copies of the largest series.

## Why The Restart Could Not Resume

The OOM landed mid-stage. On restart the resume path found the staged database
present but semantically empty and raised

```
staged migration is incomplete or invalid: {:ok, []}
```

rather than discarding and restarting the stage. So a memory failure converted
into a hard, self-perpetuating boot failure. Worth treating as its own defect:
a staged migration that fails validation should be reclaimable automatically,
since the source is retained precisely to make that safe.

## The Fix

**The paged read already exists.** `lib/timeless_metrics/rust_engine/nif.ex:45`:

```elixir
def engine_legacy_query_page(_reader, _metric, _labels, _after, _limit)
```

The migration calls the unbounded `engine_query_range` instead.

Proposed changes:

1. **Page the source.** Drive the write loop from `engine_legacy_query_page/5`
   so a bounded window is resident at a time. Peak becomes a function of page
   size, not of the largest series.
2. **Count while writing.** Accumulate from each page and drop `length/1`, so
   no full list has to stay reachable.
3. **Stream verification.** Compare page-by-page in the same window order, or
   fold both sides into running checksums, instead of materialising source and
   target together and zipping. Bit-exactness is preserved by hashing the same
   `float_bits/1` representation incrementally.
4. **Avoid the per-point binary in sorts.** If the source already returns points
   in timestamp order, the sort can be dropped; otherwise sort with a comparator
   rather than an allocating key function.

Expected result: peak memory proportional to page size, which makes the
conversion viable on a 3.9 GB host without swap.

## Measured Result

Implemented 2026-08-09. One series, all points in it, peak BEAM memory sampled
across `LibsqlMigration.run/1` on an M-series laptop:

| points | before (peak Δ) | after (peak Δ) | before (time) | after (time) |
|---|---|---|---|---|
| 1,000,000 | 628.5 MB | **31.1 MB** | 1,166 ms | 5,654 ms |

Memory is now flat in the size of the series, which was the goal:

| points | peak Δ | elapsed |
|---|---|---|
| 500,000 | 27.1 MB | 1,662 ms |
| 1,000,000 | 31.6 MB | 5,690 ms |
| 2,000,000 | 32.6 MB | 20,660 ms |

A 20× reduction in peak memory, and the OOM class of failure is gone: peak no
longer tracks the largest series.

## Superseded: See "The Read Path Was The Problem" below

The first cut of this change was memory-bounded but quadratic in time. The
cause turned out to be in the legacy read path, not in the migration, and is
fixed. The section immediately below records the original diagnosis; the final
numbers are at the end.

## Original Problem: The Paged Read Is Quadratic

Those same numbers show the cost. Doubling the points multiplies elapsed time by
~3.5, not by 2 — the conversion is now roughly **O(n²) per series**.

The cause is in `legacy_query_page` itself. Each call must return the globally
smallest `limit` points after the cursor, ordered by
`(timestamp, value_bits, path, offset, ordinal)`. Points in a later chunk can
carry earlier timestamps, so producing that global order requires visiting
**every chunk on every page**. With `n / 4096` pages each scanning all chunks,
total work grows with the square of the series size.

What this means in practice depends entirely on the per-series distribution,
which is still unmeasured:

- At the production **average** of ~142k points per series, the quadratic term
  is small and the whole conversion should finish faster than the four minutes
  it took with swap.
- A single multi-million-point series would dominate everything. Extrapolating
  the curve, one 10M-point series alone would take on the order of ten minutes.

**Validate against the retained production store before deploying this.** The
source is preserved at `/observability/metrics/rust_engine` on the production
host, so the real distribution can be measured directly rather than guessed.

## The Read Path Was The Problem

The quadratic cost was not inherent to paging. It was three defects in
`legacy_query_page` in `native/tms_engine/src/lib.rs`, all on the read side —
nothing about ingest or the on-disk format is involved.

Measured directly: a 1M-point series is stored as **one chunk**, and successive
pages cost a flat ~19 ms each. Flat per-page cost over `n / 4096` pages is what
made the total quadratic.

**1. No chunk pruning.** The normal `query_range` path prunes before decoding:

```rust
.filter(|(_, meta)| meta.min_ts <= t_end && meta.max_ts >= t_start)
```

`legacy_query_page` filtered on partition alone and then called
`read_chunk_data_cached(&meta, i64::MIN, i64::MAX, ..)` — explicitly the entire
range — even though every `ChunkMeta` already carries `min_ts` / `max_ts` and
the cursor already establishes a timestamp floor. Metas were also sorted by
`(path, data_offset)`, so there was no early exit even in principle. Now sorted
by `min_ts`, with a skip for chunks that end before the cursor and a `break`
once the retained heap is full and the remaining chunks start beyond it.

**2. Chunks were re-decoded on every page.** `file_cache` avoided the
`fs::read`, but the pco decompression ran on every call. The legacy reader now
holds the most recently decoded chunk, which is enough because the migration
advances its cursor monotonically. The cache lives on `LegacyReaderResource`,
not on `Engine`, so the ingest path is untouched.

**3. The dominant cost: an allocation per point per page.** The inner loop built
a full `LegacyPointKey` — including `relative_path.clone()`, a `String` — for
*every* point of the chunk, on *every* page, only to discard nearly all of them.
At 1M points and 245 pages that is ~245M string allocations. Points are ranked
by timestamp first, so two integer comparisons now settle the outcome before
anything is allocated.

Fixes 1 and 2 together bought only ~25%. Fix 3 is what mattered.

## Final Measured Result

Peak BEAM memory and wall clock across `LibsqlMigration.run/1`, one series:

| points | before (peak) | after (peak) | before (time) | after (time) |
|---|---|---|---|---|
| 500,000 | — | 30.9 MB | — | 538 ms |
| 1,000,000 | 628.5 MB | **32.6 MB** | 1,166 ms | **1,081 ms** |
| 2,000,000 | 1,101.4 MB | **29.3 MB** | 2,152 ms | 2,326 ms |
| 4,000,000 | 2,353.2 MB | **29.2 MB** | 4,094 ms | 5,335 ms |

Memory is flat where it used to grow linearly — **80× lower at 4M points** — and
wall clock is now comparable to the original rather than 5× worse. A 4M-point
series alone used to need 2.35 GB, on a host with 3.9 GB.

### Residual

Time is still mildly superlinear (~2.3× per doubling at 4M): each page still
scans the whole chunk, now with only two integer comparisons per point. Making
it fully linear means resuming the scan near the cursor instead of restarting
it — a binary search on timestamp for chunks that are stored in timestamp
order, falling back to a full scan for chunks that are not. Extrapolated, a
single 20M-point series would spend roughly a minute in that scan, so this is
worth doing before any store with very large individual series is migrated.
