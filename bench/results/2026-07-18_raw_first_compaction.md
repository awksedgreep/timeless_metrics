# Benchmark Results — Raw-First Flush + Background Compaction (experiment)

**Date:** 2026-07-18
**Branch:** perf/raw-first-compaction
**Machine:** Intel Core Ultra 9 185H ("i185"), Linux, 22 schedulers
**Workload:** 2,000 series x 20 scrape-style rounds x 50 pts (2M points),
realistic counter/gauge value shapes, 4 labels/series
**Benchmark script:** `bench/compaction_bench.exs`

## Design under test

With `defer_compression: true`, flushes write chunks with RAW payloads
(PCO1/PCB1 version byte 2 — same containers, same index) and the periodic
compactor (cold-flush timer, or `RustEngine.compact/1`) merges each
series' raw + undersized chunks into large pco chunks at level 12.

## Results

| Strategy                 | Flush (hot path) | Compact (bg) | Bytes/point | Storage delta |
|--------------------------|-----------------:|-------------:|------------:|--------------:|
| baseline (pco@8 @ flush) | 850ms            | —            | 5.331       | —             |
| raw-first + compaction   | **154ms**        | 1222ms       | **3.738**   | **-29.9%**    |
| ideal (single big flush) | 259ms            | —            | 3.762       | -29.4%        |

- **Hot-path flush cost drops 5.5x** — raw writes remove pco from the
  ingest path entirely (this is the same cost that walled the 1M-series
  workload at 3.9M pts/s)
- **Storage shrinks 30%**, landing at/below the ideal-chunk-size line:
  compaction fully recovers the small-chunk penalty, and pco@12 on large
  chunks edges past big-chunk pco@8
- Compaction cost (1.2s per 2M points here) runs on the background timer
- Confirms the small-chunk diagnosis for the real-world "84%" ratio:
  same codec, better-fed

## Correctness / crash-safety notes

- Rust tests: raw round-trip + restart index rebuild (version byte),
  multi-chunk merge, shared PCB1 batch handling with reference-counted
  file deletion. Full Elixir suite green.
- Known crash window: a crash after the compacted chunk is written but
  before old files are deleted resurrects duplicates via rebuild_index.
  Production hardening wants a compaction manifest (or startup dedup).
- Disk write amplification ~7x raw-vs-final (trivial on NVMe; note for
  modest disks). Raw window on disk is bounded by compaction cadence.

## Next steps before merge

1. Startup dedup or manifest for the crash window
2. Honor the existing TIMELESS_DEFER_COMPRESSION config end to end
3. Rerun the 1M-series workload with defer on — expect the 3.9M pts/s
   saturation wall to move
4. Codec shootout in the compaction slot (pco@12 vs ALP/Vortex) on real
   website chunk data

## Query regression verification (added same day)

Concern: large compacted chunks decompress fully to serve narrow windows.
Initial run confirmed it: narrow +836%, fanout-narrow +1032% vs baseline.

Fix: **age-gated compaction** (chunks younger than COMPACT_MIN_AGE_SECS =
1h are never compacted; `compact/2` takes an explicit cutoff) plus output
cap 512K -> 32K points. Recent-window queries hit small raw chunks —
no decompression at all.

Results after the fix (500 series x 20K pts, medians of 5):

| Shape          | baseline | compacted | delta  |
|----------------|---------:|----------:|-------:|
| narrow         | 630us    | 276us     | -56%   |
| fanout narrow  | 1.05ms   | 893us     | -15%   |
| full range     | 21.8ms   | 15.5ms    | -29%   |
| aggregate      | 186us    | 130us     | -30%   |
| mid (10%)      | 2.55ms   | 6.5ms     | +155%* |
| fanout full    | 105ms    | 117ms     | +12%   |

\* window clipping the edge of a compacted chunk; 130us/series absolute,
bounded by the 32K cap; time-bucketed compaction outputs would remove it.

Storage with the spared hour: 4.63 B/pt (approaches the fully-compacted
4.03 as retention grows relative to the 1h raw window).

Standard benches on the branch (defer OFF) vs recorded audit: range
50.1ms (was 51.4), NIF range 47.3 (was 53.4), aggregate 27.4 (was 29.6),
fused ingest 7.8ms/10K — no regression with the feature disabled.

Side finding (main, unrelated to branch): `TimelessMetrics.query_aggregate/4`
routes to the legacy registry path even for Rust-engine stores.
