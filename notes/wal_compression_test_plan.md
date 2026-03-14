# WAL + Compression Test Plan

## Goal

Find the optimal combination of compression level and block size for WAL-based ingest. These are intertwined — larger blocks compress better but take longer to fill, and compression level affects both throughput and ratio.

## Test Matrix

Fixed parameters:
- 200K series (10K devices × 20 metrics)
- 300 points per HTTP request
- Writers = schedulers_online (10 locally)
- Seed 10M points, then measure 60s writes + queries
- All tests on same local machine, same data pattern

Variable parameters:

| Compression Level | Block Size (points per segment) |
|---|---|
| 1 (fastest) | 1,000 (current default) |
| 2 (proposed fast) | 5,000 |
| 5 (balanced) | 10,000 |
| 9 (current default) | 50,000 |
| 15 (proposed merge) | 100,000 |

Full matrix = 5 levels × 5 block sizes = 25 tests. Too many.

## Reduced Matrix

Test in two phases to isolate variables:

### Phase 1: Compression Level (fix block size at 10,000)

Hold block size constant at 10,000 points. Vary compression level only.

| Test | Level | Block Size | Measures |
|------|-------|------------|----------|
| L1 | 1 | 10,000 | Write pts/s, bytes/point, query latency |
| L2 | 2 | 10,000 | " |
| L5 | 5 | 10,000 | " |
| L9 | 9 | 10,000 | " |
| L15 | 15 | 10,000 | " |

This tells us: how much does compression level cost in throughput, and how much does it save in storage?

### Phase 2: Block Size (fix compression at winner from Phase 1)

Take the best level from Phase 1 (likely level 2 for throughput or level 5 for balance). Vary block size only.

| Test | Level | Block Size | Measures |
|------|-------|------------|----------|
| B1K | best | 1,000 | Write pts/s, bytes/point, query latency |
| B5K | best | 5,000 | " |
| B10K | best | 10,000 | " |
| B50K | best | 50,000 | " |
| B100K | best | 100,000 | " |

This tells us: how much does block size improve compression, and does it hurt query latency (bigger blocks = more to decompress)?

### Phase 3: Confirm (optional)

Run the best combination from Phase 1+2 against the current defaults (level 9, block 1,000) to confirm the improvement.

## Metrics Per Test

| Metric | How |
|--------|-----|
| Write throughput (pts/s) | Harness final results |
| Write latency (ms/req) | Harness final results |
| Compression ratio (bytes/pt) | `du -sh` data dir / total points from health endpoint |
| Query latency (ms) | Harness final results |
| Query throughput (q/s) | Harness final results |

## Implementation

Write a single benchmark script that:
1. Takes `--level N --block-size N` parameters
2. Creates a fresh store with those settings
3. Seeds 10M points
4. Measures 60s writes + queries
5. Flushes, captures storage
6. Prints one-line summary: `level=N block=N write_pts/s=X bytes/pt=X query_lat=Xms`

Then a wrapper script runs Phase 1, picks the winner, runs Phase 2.

## What This Tells Us

- **Level 2 vs 9 vs 15**: is the throughput difference worth the compression difference?
- **1K vs 100K blocks**: do bigger blocks meaningfully improve compression ratio?
- **Query impact**: do bigger blocks hurt query latency (more to decompress per read)?
- **The sweet spot**: which combination gives us the best throughput × compression × query balance?

## Not Tested Here

- WAL accept-then-process (that's the next step after we know optimal compression settings)
- Remote/NVMe performance (local only to save cost)
- Background recompression (level 2 on ingest → level 15 on merge)
