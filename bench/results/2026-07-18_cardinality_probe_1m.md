# Cardinality Probe — 1M Series Against Container 0.6.4

**Date:** 2026-07-18
**Image:** ghcr.io/awksedgreep/timeless-stack:0.6.4 (timeless_metrics 6.1.1)
**Workload:** 50,000 devices x 20 metrics = 1.0M series, batch 50 devices/POST
**Benchmark script:** `bench/realistic_workload.exs --devices 50000 --metrics 20 --batch 50`

## Results

| Interval | Pts/s  | Write p50 | Write p99 | Query p99 |
|----------|-------:|----------:|----------:|----------:|
| 4.0s     | 249.8K | 864us     | 1.49ms    | 3.18ms    |
| 1.0s     | 999.7K | 752us     | 1.65ms    | 2.23ms    |
| 500ms    | 2.0M   | 657us     | 1.47ms    | 1.98ms    |
| 250ms    | 3.9M   | 1.14ms    | 168ms     | 6.86ms    |

- **1.0M series created cleanly** (verified); 10.2K queries, 0 errors
- **2.0M pts/s sustained at 1M series with write p99 1.47ms** — sub-2ms
  tails held through the 2M step
- Saturation at 3.9M pts/s was **server-side** (write p99 spiked to 168ms)
  — unlike the 10K-series run which was client-bound; consistent with
  flush/buffer pressure across 1M partitions
- **Memory: container RSS 372MB → 3.83GB ≈ ~3.4KB per series** (registry
  string duplication + per-series buffer bookkeeping + caches)

## Implications for 10M+ cardinality

- Throughput and latency are NOT the constraint at 1M — memory is.
  Extrapolated 10M series ≈ ~34GB RSS with current structures.
- Priority order for cardinality growth (see architecture notes):
  string interning in the series registry, append-only series log,
  roaring bitmap label index, compact ChunkMeta (drop per-chunk PathBuf),
  cache eviction, idle-buffer reaping.
- Single-node 10M looks feasible after interning + index slimming
  (~2-5x memory reduction); sharding not needed before ~5M today.
