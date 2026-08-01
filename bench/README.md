# Benchmarks

This directory contains the current benchmark set for `timeless_metrics`.

> **New machine?** Run `mise trust && mise install` in the repo root first.
> An untrusted `mise.toml` silently falls back to the machine's global
> Erlang/Elixir — an OTP 28 client generates ~15% less load than the
> pinned OTP 29 toolchain and skews every HTTP benchmark low (see
> `results/2026-07-18_cardinality_bank_14700hx_v0.6.6.md`).
>
> For the cross-machine cardinality bank, use `cardinality_bank.sh` —
> methodology reference in `results/2026-07-18_cardinality_bank_i185_v0.6.5.md`.

## Benchmarks

- [write_bench.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/write_bench.exs)
  Embedded API benchmark for write throughput, flush/compression cost, query latency, and storage footprint.
  Use this for quick local regression checks on the core engine.
  Pass `--libsql` to run the same workload against the opt-in libSQL engine.

- [rust_engine_baseline.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/rust_engine_baseline.exs)
  Focused Rust-engine benchmark for labeled writes, new-series creation, raw binary writes, flush cost, and targeted query latency.
  Use this before and after Rust/Rustler hot-path changes.

- [rust_query_bench.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/rust_query_bench.exs)
  Focused Rust-engine query benchmark for high-fanout multi-series range and aggregate queries over flushed batch files.
  Use this when changing Rust query internals, file reuse, or decode behavior.

- [engine_query_bench.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/engine_query_bench.exs)
  Reproducible public-API comparison for the Rust and libSQL engines, covering first-read publication cost, exact/narrow/wide raw reads, scalar and bucketed aggregation, latest queries, and libSQL boundary attribution.
  Wide raw, scalar aggregate, and wide latest queries also sample their
  worker-process peaks, including referenced binaries. The wide raw path
  enforces a 10x bound relative to the serialized result size.
  Run each engine in a separate process with `MIX_ENV=test mix run bench/engine_query_bench.exs --engine rust|libsql`.

- [engine_query_distribution.sh](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/engine_query_distribution.sh)
  Runs the public Rust/libSQL query workload in five fresh processes per
  engine by default and emits one CSV row per process and query shape. Use this
  for release decisions so BEAM-process variance is not hidden.

- [libsql_soak.exs](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/libsql_soak.exs)
  Mixed libSQL release soak with live writes, multiple oracle readers,
  compaction, rollup, retention, online backup, a forced writer restart,
  backup restore, and primary-store reopen. Run with
  `MIX_ENV=test mix run bench/libsql_soak.exs --seconds 30 --readers 8`.

- [rollup_query_bench.exs](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/rollup_query_bench.exs)
  Focused comparison of the former six-row-query rollup adapter and the packed
  `timeless_rollup_batches` path over at least 1,000 settled buckets.
  Run with `MIX_ENV=test mix run bench/rollup_query_bench.exs --runs 30 --buckets 1200`.

- [rust_series_bench.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/rust_series_bench.exs)
  Focused Rust-engine benchmark for new-series resolution, cached series lookup, and first-write cost for unseen series.
  Use this when changing series-id caching or series registry persistence behavior.

- [http_concurrency.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/http_concurrency.exs)
  HTTP ingest concurrency benchmark for Prometheus text and JSON line imports under mixed read load.
  Use this when changing the HTTP ingest path or request handling.
  Note: the current read side uses `/health/detailed` as the stable GET path while rust HTTP query benchmarks are still being hardened.

- [realistic_workload.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/realistic_workload.exs)
  Deterministic end-to-end HTTP workload generator with ramp-up behavior,
  separate offered/admitted/completed throughput, queue age/depth, and an
  explicit final drain barrier. Use this to find saturation points and observe
  throughput/latency tradeoffs under a more production-like traffic mix.

- [http_baseline_server.exs](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/http_baseline_server.exs)
  Starts one isolated Elixir HTTP control over either libSQL or the Rust block
  engine, with configurable readers/workers and optional deferred scheduled
  maintenance. Use it with `realistic_workload.exs` for fresh-process API
  comparisons; compile once, then run both processes with `--no-compile`.

- [tsbs_bench.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/tsbs_bench.exs)
  TSBS harness that starts a local TimelessMetrics HTTP endpoint and prints the commands needed to run the external TSBS tools.
  Use this for standardized TSBS comparisons.

- [vs_victoriametrics.exs](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/vs_victoriametrics.exs)
  Comparison benchmark against VictoriaMetrics covering native ingest, HTTP ingest, storage, and query latency.
  Use this when making competitive or positioning claims.

## Guidance

- Prefer `write_bench.exs` for engine-only regressions.
- Prefer `rust_engine_baseline.exs` when changing Rust NIF write/query internals.
- Prefer `rust_query_bench.exs` when changing multi-series Rust query internals.
- Prefer `rollup_query_bench.exs` when changing stored-tier query transport or decoding.
- Prefer `rust_series_bench.exs` when changing new-series resolution or series registry persistence.
- Prefer `http_concurrency.exs` or `realistic_workload.exs` for HTTP path changes.
- Prefer `tsbs_bench.exs` when you want an external, standardized workload.
- Prefer `vs_victoriametrics.exs` only when you specifically need a product-to-product comparison.

## Historical Results

- [results/2026-08-01_metrics_api_session0.md](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-08-01_metrics_api_session0.md)
  Rust metrics API POC Session 0 compatibility contract and completion-aware
  fresh-process Elixir+libSQL/Rust-block baseline.

- [results/2026-08-01_standalone_query_adoption.md](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-08-01_standalone_query_adoption.md)
  Session 6 adoption of selected-ID reads and the TAF1/TLF1 aggregate/latest
  transports, including controlled row fallback latency and peak memory.

- [results/2026-07-31_integration_release_gate.md](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-07-31_integration_release_gate.md)
  Five-process Rust/libSQL query distributions, the large write/storage gate,
  packed-rollup distribution, mixed soak, and the default-engine decision.

- [results/2026-07-31_matcher_discovery_pushdown.md](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-07-31_matcher_discovery_pushdown.md)
  Hybrid matcher planning, filtered public discovery, semantic fallbacks, and
  the selective-query Rust/libSQL comparison.

- [results/2026-07-31_catalog_publication.md](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-07-31_catalog_publication.md)
  Transaction-safe catalog-generation publication and the first-query latency
  result through the public TimelessMetrics API.

- [results/2026-07-31_raw_frame.md](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-07-31_raw_frame.md)
  One-row raw-frame transport, native final-map decoding, stage attribution,
  and the enforced public-query memory bound.

- [results/2026-07-31_packed_rollups.md](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-07-31_packed_rollups.md)
  One-call packed rollup adapter implementation, parity coverage, and the
  1,200-bucket six-query comparison.

- [results/2026-07-31_native_bucketed.md](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-07-31_native_bucketed.md)
  Native complete-bucket adoption, packed-window boundary attribution, and the
  final Rust/libSQL comparison.

- [results/2026-07-31_native_aggregate.md](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-07-31_native_aggregate.md)
  Native scalar aggregate implementation result and extension/adapter
  attribution.

- [results/2026-07-31_libsql_query_baseline.md](/home/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-07-31_libsql_query_baseline.md)
  First extension-first Rust/libSQL public-query baseline, including direct
  read-boundary attribution and fresh-process variance.

- [results/2026-03-12_ets_optimizations.md](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/results/2026-03-12_ets_optimizations.md)
  Archived benchmark notes from an earlier optimization pass.
