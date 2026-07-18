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
