# Benchmark Results — Fused Prometheus Ingest NIF

**Date:** 2026-07-18
**Version:** 6.1.2-dev (engine_ingest_prometheus)
**Workload:** Generated exposition bodies, 8 labels/series, warm series cache
**Benchmark script:** `bench/ingest_segments_bench.exs`
**Machine:** Intel Core Ultra 9 185H ("i185"), Linux, 22 schedulers

## What changed

`engine_ingest_prometheus(body, default_ts)` parses Prometheus text and
resolves + buffers every sample inside one DirtyCpu NIF call. No BEAM terms
are built per sample; the cache-hit path performs no allocations per sample.
The resolve cache is shared with the labeled write path (hash-identical
keys, verified against the registry to rule out collisions). Wired into the
HTTP `/api/v1/import/prometheus` path (relabel-free by definition); the
scraper keeps the term path because it applies relabel rules.

## Scoreboard (median per body)

| Body | Old path (parse + write_batch) | Fused  | Speedup | GC old → fused |
|------|-------------------------------:|-------:|--------:|---------------:|
| 1K   | 2.29ms                         | 0.67ms | 3.4x    | 260us → 1us    |
| 10K  | 22.1ms                         | 8.3ms  | 2.7x    | 3.6ms → 5us    |
| 50K  | 108ms                          | 42.9ms | 2.5x    | 27.5ms → 11us  |

- **~1.2M pts/s through a single fused call** (10K samples / 8.3ms),
  vs ~450K pts/s for the old pipeline — parse+resolve+write now costs
  barely more than the old path's parse step alone
- Per-sample BEAM garbage eliminated (µs of GC instead of ms)
- Remaining per-sample cost ≈ 600ns: cache verify (registry read lock) +
  DashMap entry + buffer push — next candidates if ever needed

## Correctness

- Rust tests: hash parity between paths, shared series ids across paths,
  duplicate-label-key last-wins, ms→s timestamp normalization
- Full Elixir suite (390) green, HTTP import tests exercise the fused path
