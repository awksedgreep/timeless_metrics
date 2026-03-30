# Rust Engine Current State

Date: 2026-03-30
Branch: `perf/rust-engine-optimizations`
Script: [`bench/rust_engine_baseline.exs`](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/rust_engine_baseline.exs)

These results were captured sequentially, one mode at a time, with no overlapping benchmark process.

## Workload

- Series: `2,000`
- Points per series: `120`
- Batch size: `500`
- Schedulers: `18`

## Results

### Memory-only

- Labeled write, cached series: `1,554,645/sec` (`154ms`)
- Raw write, pre-resolved ids: `8,918,949/sec` (`26ms`)
- Labeled write, new series: `4,037/sec` (`495ms`)
- Flush: `212ms`
- Single-series query: `240 pts in 0ms`
- Multi-series query: `334 series in 6ms`
- Aggregate query: `334 series in 0ms`
- Storage bytes: `1,192,075`
- Bytes per point: `2.473`
- Disk points: `482,000`

### Disk

- Labeled write, cached series: `1,531,002/sec` (`156ms`)
- Raw write, pre-resolved ids: `8,748,906/sec` (`27ms`)
- Labeled write, new series: `3,999/sec` (`500ms`)
- Flush: `199ms`
- Single-series query: `240 pts in 0ms`
- Multi-series query: `334 series in 6ms`
- Aggregate query: `334 series in 0ms`
- Storage bytes: `1,192,567`
- Bytes per point: `2.474`
- Disk points: `482,000`

## Interpretation

- The current branch has a solid steady-state write path for known series.
- The remaining dominant write bottleneck is new-series creation and persistence.
- No storage format changes were made in this step, so existing on-disk compatibility is unchanged.
