# Cardinality Bank — 100K / 250K / 500K / 1M Series (i185)

**Date:** 2026-07-18
**Machine:** Intel Core Ultra 9 185H ("i185"), Linux, 22 schedulers
**Image:** ghcr.io/awksedgreep/timeless-stack:0.6.4 (timeless_metrics 6.1.1 — pre-fused-NIF baseline)
**Method:** Fresh ephemeral container + empty data dir per scale (no series reuse
across runs). Client is a separate host BEAM.
**Benchmark script:** `bench/realistic_workload.exs --devices D --metrics 20 --batch 50 --step-seconds 15`
(D = 5000 / 12500 / 25000 / 50000 → series = 20×D)

## Summary

| Series | Peak pts/s | Clean zone (write p99 < 2ms) | Saturation mode        | RSS after |
|-------:|-----------:|-----------------------------:|------------------------|----------:|
| 100K   | 5.9M       | 3.0M pts/s                   | ramp shortfall (41%)   | 3.26GB    |
| 250K   | 6.3M       | 2.0M pts/s                   | ramp shortfall (37%)   | 4.33GB    |
| 500K   | 6.4M       | 2.0M pts/s (3.9M @ 3.2ms)    | ramp shortfall (39%)   | 5.13GB    |
| 1M     | 3.9M       | 2.0M pts/s                   | server write p99 137ms | 4.02GB    |

- All scales: **0 query errors**; all series counts verified exact
- **2.0M pts/s with sub-2ms write p99 holds at every scale through 1M series**
- Peak throughput is flat ~6M pts/s from 100K–500K, dropping to 3.9M at 1M
  (flush fanout across 1M partition buffers becomes the wall)

## Write latency by scale (selected steps)

| Series | @1M pts/s p99 | @2M p99 | @3–4M p99 | @peak p50/p99 |
|-------:|--------------:|--------:|----------:|--------------:|
| 100K   | ~1.3ms        | 1.5ms*  | 1.96ms (3.0M) | 6.4ms / 15.6ms (5.9M) |
| 250K   | 1.52ms        | 1.39ms  | 3.53ms (3.8M) | 18.7ms / 42.9ms (6.3M) |
| 500K   | 1.65ms        | 1.42ms  | 3.23ms (3.9M) | 41.0ms / 72.7ms (6.4M) |
| 1M     | 1.65ms        | 1.47ms  | 137ms (3.9M)  | 1.2ms / 137ms (3.9M)  |

\* interpolated between 1.6M (1.30ms) and 3.0M (1.96ms) steps

## Query latency under write load (concurrent, 20 workers)

Query p99 stays 1.4–2.5ms at every scale while writes are inside the clean
zone; it degrades in lockstep with write latency past saturation (worst:
86ms p99 at 500K/6.4M pts/s). Isolated slow steps (e.g. 20ms p99 at
250K/125K pts/s) coincide with cold-populate flush activity early in runs.

## Memory interpretation

"RSS after" is dominated by the write-buffer high-water mark at saturation,
not cardinality: the 500K run (5.13GB) exceeds the 1M run (4.02GB) because
it sustained 6.4M pts/s vs 3.9M before saturating — more points buffered
uncompressed at once. Cardinality-attributable footprint is better read
from the dedicated 1M probe (~3.4KB/series; see
2026-07-18_cardinality_probe_1m.md). For bounded-memory deployments set
`memory_budget_mb` to cap buffer growth under burst load.

## Cross-machine comparison

Planned reruns of this exact bank on Apple Silicon and a faster Intel
host — same script, same flags, only the Machine line changes.
