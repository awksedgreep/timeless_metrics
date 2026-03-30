# Benchmarks

This directory contains the current benchmark set for `timeless_metrics`.

## Benchmarks

- [write_bench.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/write_bench.exs)
  Embedded API benchmark for write throughput, flush/compression cost, query latency, and storage footprint.
  Use this for quick local regression checks on the core engine.

- [rust_engine_baseline.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/rust_engine_baseline.exs)
  Focused Rust-engine benchmark for labeled writes, new-series creation, raw binary writes, flush cost, and targeted query latency.
  Use this before and after Rust/Rustler hot-path changes.

- [http_concurrency.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/http_concurrency.exs)
  HTTP ingest concurrency benchmark for Prometheus text and JSON line imports under mixed read load.
  Use this when changing the HTTP ingest path or request handling.
  Note: the current read side uses `/health/detailed` as the stable GET path while rust HTTP query benchmarks are still being hardened.

- [realistic_workload.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/realistic_workload.exs)
  End-to-end HTTP workload generator with ramp-up behavior.
  Use this to find saturation points and observe throughput/latency tradeoffs under a more production-like traffic mix.

- [tsbs_bench.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/tsbs_bench.exs)
  TSBS harness that starts a local TimelessMetrics HTTP endpoint and prints the commands needed to run the external TSBS tools.
  Use this for standardized TSBS comparisons.

- [vs_victoriametrics.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/vs_victoriametrics.exs)
  Comparison benchmark against VictoriaMetrics covering native ingest, HTTP ingest, storage, and query latency.
  Use this when making competitive or positioning claims.

## Guidance

- Prefer `write_bench.exs` for engine-only regressions.
- Prefer `rust_engine_baseline.exs` when changing Rust NIF write/query internals.
- Prefer `http_concurrency.exs` or `realistic_workload.exs` for HTTP path changes.
- Prefer `tsbs_bench.exs` when you want an external, standardized workload.
- Prefer `vs_victoriametrics.exs` only when you specifically need a product-to-product comparison.

## Historical Results

- [results/2026-03-12_ets_optimizations.md](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-03-12_ets_optimizations.md)
  Archived benchmark notes from an earlier optimization pass.
