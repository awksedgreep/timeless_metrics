# Rust Engine Baseline

Invalidated: this note was captured from parallel benchmark execution of memory and disk modes and should not be used for performance comparisons.

Date: 2026-03-30
Branch: `perf/rust-engine-optimizations`
Script: [`bench/rust_engine_baseline.exs`](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/rust_engine_baseline.exs)

## Workload

- Series: `2,000`
- Points per series: `120`
- Batch size: `500`
- Schedulers: `18`

The benchmark measures:

- Labeled writes against already-known series
- Raw binary writes using pre-resolved series ids
- Labeled writes that create new series
- Flush latency
- Single-series and multi-series query latency after flush

## Results

### Memory-only

- Labeled write, existing series: `490,683/sec` (`489ms`)
- Raw write, pre-resolved ids: `5,729,154/sec` (`41ms`)
- Labeled write, new series: `4,041/sec` (`494ms`)
- Flush: `290ms`
- Single-series query: `280 pts in 0ms`
- Multi-series query: `334 series in 8ms`
- Aggregate query: `334 series in 0ms`
- Storage bytes: `1,222,233`
- Bytes per point: `2.525`
- Disk points: `484,100`

### Disk

- Labeled write, existing series: `551,193/sec` (`435ms`)
- Raw write, pre-resolved ids: `5,832,887/sec` (`41ms`)
- Labeled write, new series: `4,088/sec` (`489ms`)
- Flush: `216ms`
- Single-series query: `240 pts in 0ms`
- Multi-series query: `334 series in 5ms`
- Aggregate query: `334 series in 0ms`
- Storage bytes: `1,192,877`
- Bytes per point: `2.475`
- Disk points: `482,000`

## Observations

- Raw writes are roughly `10x-12x` faster than labeled writes on existing series. This supports prioritizing the `write_batch_raw` path and reducing label-resolution overhead.
- New-series creation is roughly `120x-140x` slower than raw writes and roughly `120x` slower than steady-state labeled writes. This strongly suggests the synchronous series registry persistence path is a major bottleneck.
- Flush time is measurable but not dominant for this workload. The main write-path gap is ahead of compression and disk persistence.
- Multi-series query latency is low at this scale, but this benchmark does not yet stress repeated reads of large shared batch files. That should be rechecked after query-side changes.

## Compatibility Note

Active users are already writing data with the current on-disk formats. Any storage change should:

- Keep readers for existing `PCO1` and `PCB1` files
- Introduce a versioned header for any new format
- Verify mixed reads across old and new files during the optimization work
