# Rust Query Aggregate Fix

Date: 2026-03-30
Branch: `perf/rust-engine-optimizations`
Script: [`bench/rust_query_bench.exs`](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/rust_query_bench.exs)

These results were captured sequentially, one mode at a time, with no overlapping benchmark process.

## Workload

- Series fanout: `12,000`
- Points per series: `60`
- Timed runs per query shape: `5`
- Schedulers: `18`

## Change

Fixed [`query_aggregate_multi/4`](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/lib/timeless_metrics/rust_engine.ex) so unbucketed multi-series aggregates use the Rust aggregate NIF directly instead of fetching full raw point sets through the range-query path.

## Results

### Memory-only

- Populate: `119,159/sec` (`6042ms`)
- Flush: `171ms`
- Range query median: `204.89ms`
- Range query best: `203.94ms`
- Range query worst: `231.11ms`
- Aggregate query median: `14.54ms`
- Aggregate query best: `10.21ms`
- Aggregate query worst: `23.62ms`

### Disk

- Populate: `116,752/sec` (`6166ms`)
- Flush: `182ms`
- Range query median: `205.96ms`
- Range query best: `204.54ms`
- Range query worst: `239.14ms`
- Aggregate query median: `15.54ms`
- Aggregate query best: `10.65ms`
- Aggregate query worst: `21.06ms`

## Comparison To Prior Query Baseline

Compared with [`2026-03-30_rust_query_current_state_sequential.md`](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-03-30_rust_query_current_state_sequential.md):

- Aggregate query median improved from about `203ms` to about `15ms`
- Range query median remained roughly flat at about `205ms`

## Interpretation

- The aggregate query path had been accidentally paying the full raw range-query cost.
- That issue is now fixed.
- The next remaining high-value query target is multi-series range-query latency.
