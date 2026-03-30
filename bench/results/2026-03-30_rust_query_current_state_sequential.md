# Rust Query Current State

Date: 2026-03-30
Branch: `perf/rust-engine-optimizations`
Script: [`bench/rust_query_bench.exs`](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/rust_query_bench.exs)

These results were captured sequentially, one mode at a time, with no overlapping benchmark process.

## Workload

- Series fanout: `12,000`
- Points per series: `60`
- Timed runs per query shape: `5`
- Schedulers: `18`

The benchmark writes one metric across all series, flushes, then measures:

- Multi-series range queries via `query_multi/4`
- Multi-series aggregate queries via `query_aggregate_multi/4`

## Results

### Memory-only

- Populate: `118,722/sec` (`6064ms`)
- Flush: `174ms`
- Warm range result: `12,000 series`
- Warm aggregate result: `12,000 series`
- Range query median: `202.48ms`
- Range query best: `202.16ms`
- Range query worst: `244.92ms`
- Aggregate query median: `202.73ms`
- Aggregate query best: `201.37ms`
- Aggregate query worst: `227.27ms`

### Disk

- Populate: `116,126/sec` (`6200ms`)
- Flush: `174ms`
- Warm range result: `12,000 series`
- Warm aggregate result: `12,000 series`
- Range query median: `207.57ms`
- Range query best: `204.61ms`
- Range query worst: `247.46ms`
- Aggregate query median: `205.09ms`
- Aggregate query best: `203.44ms`
- Aggregate query worst: `229.46ms`

## Interpretation

- High-fanout multi-series query latency is currently about `200ms` for `12,000` matching series.
- Disk mode is only slightly slower than memory mode in this workload, which suggests the current bottleneck is not dominated by raw file I/O alone.
- The next likely optimization targets are decode cost, allocation cost, and per-series result construction in the Rust query path.
