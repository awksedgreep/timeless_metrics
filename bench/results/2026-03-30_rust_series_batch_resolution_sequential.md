# Rust Series Batch Resolution

Date: 2026-03-30
Branch: `perf/rust-engine-optimizations`
Script: [`bench/rust_series_bench.exs`](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/rust_series_bench.exs)

These results were captured sequentially, one mode at a time, with no overlapping benchmark process.

## Workload

- Series: `20,000`
- Schedulers: `18`

The benchmark measures:

- Resolving a large set of unseen series
- Resolving the same series again through the cache
- Writing one point for a large set of unseen series
- Flush cost after the first-write burst

## Change

Added a batched Rust NIF for series resolution and routed Rust-engine cache misses through it, so new series are resolved under one registry lock and one NIF crossing per batch instead of one per series.

## Results

### Memory-only

- Resolve new series: `486,558/sec` (`41ms`)
- Resolve cached series: `3,808,073/sec` (`5ms`)
- First write for new series: `410,686/sec` (`48ms`)
- Flush after first write: `104ms`
- Resolved ids stable: `true`

### Disk

- Resolve new series: `501,102/sec` (`39ms`)
- Resolve cached series: `3,570,790/sec` (`5ms`)
- First write for new series: `414,585/sec` (`48ms`)
- Flush after first write: `103ms`
- Resolved ids stable: `true`

## Comparison To Prior Series Baseline

Relative to the earlier sequential run of the same benchmark:

- New-series resolution improved from about `1.3K/sec` to about `487K-501K/sec`
- First-write throughput for unseen series improved from about `1.2K/sec` to about `411K-415K/sec`
- Cached resolution remained fast and flush cost remained roughly unchanged

## Interpretation

- The old bottleneck was primarily per-series miss handling across the NIF boundary and repeated registry locking.
- Batched miss resolution removes that bottleneck without changing on-disk storage format.
- This likely closes out the main new-series creation performance problem for the current architecture.
