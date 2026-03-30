# Rust Engine Write Path Step 1

Invalidated: this note was captured from parallel benchmark execution of memory and disk modes and should not be used for performance comparisons.

Date: 2026-03-30
Branch: `perf/rust-engine-optimizations`
Change set:

- Added an Elixir-side ETS series-id cache in [`lib/timeless_metrics/rust_engine.ex`](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/lib/timeless_metrics/rust_engine.ex)
- Routed Rust-engine writes through `engine_write_batch_raw` once series ids are known
- Updated the ingest worker hot path in [`lib/timeless_metrics/ingest_worker.ex`](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/lib/timeless_metrics/ingest_worker.ex)
- Added public `resolve_series` / `write_resolved` coverage for the Rust engine
- Updated [`bench/rust_engine_baseline.exs`](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/rust_engine_baseline.exs) so cached-series numbers are actually prewarmed steady-state writes

## Workload

- Series: `2,000`
- Points per series: `120`
- Batch size: `500`
- Schedulers: `18`

## Results

### Memory-only

- Labeled write, cached series: `1,547,448/sec` (`155ms`)
- Raw write, pre-resolved ids: `8,775,137/sec` (`27ms`)
- Labeled write, new series: `3,965/sec` (`504ms`)
- Flush: `199ms`
- Single-series query: `240 pts in 0ms`
- Multi-series query: `334 series in 6ms`
- Aggregate query: `334 series in 0ms`

### Disk

- Labeled write, cached series: `1,551,961/sec` (`154ms`)
- Raw write, pre-resolved ids: `8,765,842/sec` (`27ms`)
- Labeled write, new series: `3,921/sec` (`509ms`)
- Flush: `207ms`
- Single-series query: `240 pts in 0ms`
- Multi-series query: `334 series in 5ms`
- Aggregate query: `334 series in 0ms`

## Comparison To Baseline

Compared with the original baseline in [`2026-03-30_rust_engine_baseline.md`](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-03-30_rust_engine_baseline.md):

- Cached labeled steady-state writes are now about `3x` faster than the earlier mixed hot/cold labeled path
- Raw writes remain about `5.6x` faster than cached labeled writes, so there is still headroom in the Elixir-side encode/cache path
- New-series write throughput is still roughly unchanged and remains the main bottleneck

## Interpretation

- The series-id cache and raw batch path materially improve steady-state writes without changing on-disk storage formats.
- The dominant remaining write bottleneck is new-series creation and persistence, not the steady-state append path.
- The next optimization target should be the series registry persistence path, but any change there needs explicit format/version compatibility for existing users.
