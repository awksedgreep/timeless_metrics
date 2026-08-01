# Native libSQL latest-point result — 2026-07-31

TimelessMetrics now routes `latest` and `latest_multi` to the extension's
public `timeless_latest` TVF. It no longer fetches and materializes complete
raw histories before choosing one point per series.

Starting revisions were `d355d3d1ed43ad2f77e90e47b7a3455d57ba9d6b`
in TimelessMetrics and `a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69`
in timeless-libsql, both on their existing feature branches with the paired
work uncommitted.

## Command and environment

```sh
MIX_ENV=test mix run bench/engine_query_bench.exs \
  --engine rust --series 12000 --points 60 --runs 20
MIX_ENV=test mix run bench/engine_query_bench.exs \
  --engine libsql --series 12000 --points 60 --runs 20
```

Core Ultra 9 185H, Linux 7.1.3, 22 schedulers, `powersave`, `/tmp` on
tmpfs, Elixir 1.20.2/OTP 29, SQLite 3.53.2. Dataset: one metric, 12,000
series, 60 points per series, 720,000 total points. Engines ran sequentially
in fresh processes.

## Side-by-side result

| Public query | Rust median | libSQL median | Relative result |
|---|---:|---:|---:|
| Exact latest, 100 runs | 0.128ms | 0.248ms | libSQL 1.94x Rust |
| Latest, all 12K series | 50.708ms | 35.782-39.285ms | libSQL 1.29-1.42x faster |

The pre-change libSQL latest range was 408.630-499.219ms, so the new route is
10.40-13.95x faster at the public API. Both the exact and full-fan-out gates
are inside the Session 2 limit of 2x Rust.

The finalized 20-run libSQL attribution sample measured:

| Stage | Median | P95 |
|---|---:|---:|
| Prepared SQLite latest fetch | 35.127ms | 38.187ms |
| ETS label/result shaping | 2.877ms | 3.919ms |
| Fetch + shape | 37.690ms | 39.744ms |
| Public `latest_multi` | 39.285ms | 43.265ms |
| No-pending writer barrier | 0.010ms | 0.022ms |

An earlier fresh 20-run process measured 35.782ms public and 33.348ms fetch;
the range is reported rather than selecting the faster process. The matching
Rust 20-run p95 was 61.389ms.

## Adapter boundary

- One latest statement is prepared per pooled reader and projects only
  `series_id`, timestamp, and value.
- Immutable labels come from the ETS series cache; anchored regex and negative
  matchers remain an application post-filter after equality pushdown.
- TVF labels serialize lazily only when SQL projects that column, benefiting
  direct and embedded users while callers should use `ORDER BY` whenever SQL
  row order matters.
- Exact raw reads use a separate no-label projection and skip the redundant
  merge sort for a single returned series; complex matchers retain the full
  matcher-aware fallback.

Direct extension results and the durable-format compatibility contract are in
`timeless-libsql/tools/bench/results/2026-07-31_native_latest.md`.

Validation:

- focused libSQL engine suite: 8 passed
- full TimelessMetrics suite: 466 passed
- extension workspace tests and all 36 direct CLI sections: passed
- three 50K-operation SQL oracles and five kill/reopen crash rounds: passed
- formatting and `git diff --check`: passed
