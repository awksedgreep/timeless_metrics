# Benchmark Results — ETS Read Buffer Optimizations
**Date:** 2026-03-12
**Commit:** 8ae63ee (ordered_set + select match spec + range-aware compression cleanup)
**Workload:** 200K series (10K devices × 20 metrics), realistic scrape-interval writes + PromQL queries
**Benchmark script:** `bench/realistic_workload.exs`

## Optimizations Applied

1. **ETS ordered_set with composite key** `{series_id, ts, seq}` — efficient range scans, pre-sorted results
2. **`:ets.select` match spec** — pushes timestamp filtering into C-level ETS code, replaces O(n) Enum.flat_map + Enum.sort_by
3. **Range-aware compression cleanup** — only deletes entries up to `last_ts` of compressed block, prevents empty-ETS window that forces GenServer.call fallback

## 10-Core Mac (Apple Silicon)

| Scrape Interval | Write Throughput | Write p50 | Write p99 | Query p50  | Query p99 |
|-----------------|-----------------|-----------|-----------|------------|-----------|
| 4s              | 50K pts/s       | —         | —         | 250μs      | —         |
| 1s              | 200K pts/s      | —         | < 6ms     | 560μs      | —         |
| ~125ms (peak)   | 1.3M pts/s      | —         | —         | —          | —         |

- Saturated at ~125ms scrape interval (far faster than any real Prometheus setup)

## i8g.48xlarge (192 vCPU, 1.5TB RAM, NVMe)

| Scrape Interval | Write Throughput | Write p50 | Write p99 | Query p50  | Query p99 |
|-----------------|-----------------|-----------|-----------|------------|-----------|
| 1s              | 200K pts/s      | 5.6ms     | 9.3ms     | 347-425μs  | —         |
| 62ms (peak)     | 1.9M pts/s      | —         | > 100ms   | sub-ms     | —         |

- Server CPU utilization at peak: ~60% across all 192 cores
- **1.9M pts/s was a client-side bottleneck** (1000 HTTP writers from single machine), not server saturation
- Estimated server capacity: 3-5M pts/s with multiple clients or bypassing HTTP
- Query p50 stayed sub-millisecond up to 1.5M pts/s write load
- Warmup: 120s with batch size 50 (200K series cold-start needs gentle ramp)
- Saturated (by client) at 62ms scrape interval

## Context

- 200K series covers ~95% of target user base (10K devices × 20 metrics)
- 90% of real-world queries target the 24-48h raw retention window
- Benchmark uses realistic node_exporter-style metric names and PromQL query patterns (instant + range)
- Single-node, single-container deployment — no cluster, no separate reader/writer processes
