# Cardinality Bank — Container 0.6.5 Re-Baseline (i185)

**Date:** 2026-07-18 (evening; morning bank was image 0.6.4)
**Machine:** Intel Core Ultra 9 185H ("i185"), Linux, 22 schedulers
**Image:** ghcr.io/awksedgreep/timeless-stack:0.6.5 (timeless_metrics 6.1.2,
logs 1.4.18, traces 1.3.16). defer_compression OFF (default).
**Method:** identical to the 0.6.4 bank — fresh ephemeral container + empty
data dir per scale, `realistic_workload.exs --batch 50 --step-seconds 15`,
scales 100K/250K/500K/1M series.

This file is the reference for cross-machine comparisons (Mac / faster
Intel): rerun the same loop against :0.6.5, change only the Machine line.

## Peak throughput vs 0.6.4

| Series | 0.6.4 peak | 0.6.5 peak | Write p99 @ 2M pts/s (0.6.4 -> 0.6.5) |
|-------:|-----------:|-----------:|--------------------------------------:|
| 100K   | 5.9M       | 5.4M       | ~1.5ms -> 1.02ms (@1.6M)              |
| 250K   | 6.3M       | 6.0M       | 1.39ms -> 1.06ms                      |
| 500K   | 6.4M       | **6.8M**   | 1.42ms -> 1.16ms                      |
| 1M     | 3.9M       | 3.9M       | 1.47ms -> 1.15ms                      |

- Peaks are **equivalent within run-to-run variance (±8%)** — expected:
  this workload uses the influx `/write` path, unchanged in 6.1.2, and
  raw-first compaction is dormant (opt-in). 500K's 6.8M is the best
  single number recorded on this machine.
- **Clean-zone latency improved ~15-20% at every scale**: write p99 at
  2M pts/s is now 1.0-1.2ms (was 1.4-1.5ms).
- 1M-series wall unchanged (3.9M pts/s, server write p99 118ms) — the
  known flush-pressure ceiling. The fix for it (raw-first flush) ships
  in this image but requires TIMELESS_DEFER_COMPRESSION=true; a defer-on
  rerun is the designated follow-up.
- 0 query errors at all scales.

## Memory note

"RSS after" ran higher than the 0.6.4 bank (e.g. 5.9GB vs 3.3GB at
100K). As established this morning, this metric is the write-buffer
high-water at the moment the ramp stops and is weakly comparable across
runs (the 0.6.4 bank itself had 500K > 1M). Baseline RSS is unchanged
(~270-300MB). Not treated as a regression signal; the defer-on rerun
(cheap flushes -> earlier draining) is the intended lever on buffer
high-water and will give a cleaner read.

## Fused ingest path

Verified live in this image post-deploy (POST /api/v1/import/prometheus
-> engine_ingest_prometheus -> queryable). This bank does not exercise
it; a prometheus-format workload variant would, if we want that number
at scale.
