# Benchmark Results — Rust Prometheus Parser + Ingest Segment Costs

**Date:** 2026-07-18
**Commit:** 8b57eda (Rust parser replaces C++ NIF), released as v6.1.0/v6.1.1
**Workload:** Generated Prometheus exposition bodies, 8 labels/series
**Benchmark script:** `bench/ingest_segments_bench.exs`
**Machine:** Intel Core Ultra 9 185H ("i185"), Linux, 22 schedulers

## Context

The C++ parser NIF (`c_src/prometheus_nif.cpp`) was replaced with a Rust
parser in `native/tms_engine`. Optimization arc for the term-building path
(10K-sample body): 35.5ms (naive, String round-trips) → 8.4ms (zero-copy
sub-binaries) → 7.3ms (one-pass streaming sink, zero steady-state
allocations).

## Final Segment Costs (median, 30 iters)

| Segment              | 1K samples | 10K samples | 50K samples |
|----------------------|-----------:|------------:|------------:|
| parse (Rust, terms)  |      695us |      7.28ms |     41.04ms |
| parse (C++, removed) |      686us |      7.08ms |     47.69ms |
| parse (count-only)   |      217us |      2.24ms |     11.03ms |
| middle/write_batch   |     1.66ms |     15.18ms |     66.62ms |
| write/raw (enc+nif)  |       69us |      1.15ms |      5.69ms |

- Rust vs C++: parity at 1K/10K (within run noise), **14% faster at 50K**
- Term materialization ≈ (terms − count-only): ~5ms per 10K samples, plus
  ~1.6ms deferred GC on the calling process
- Elixir middle (normalize/resolve/encode) ≈ write_batch − raw: ~14ms/10K —
  the dominant ingest cost and the target of a future fused
  parse→resolve→write NIF (projected ~4x on the ingest slice)

## Notes

- Rust parser emits zero-copy sub-binaries of the request body; long-lived
  stores copy at the boundary (`:binary.copy` in series cache et al.)
- Three intentional strictness differences vs the old strtod/strtoll
  behavior are pinned in `test/prometheus_parser_edge_test.exs` (hex floats
  rejected, timestamp overflow → 0 sentinel, no 64-char field limit)
