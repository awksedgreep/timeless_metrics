# Native libSQL bucketed-query result — 2026-07-31

TimelessMetrics now routes complete `from`-aligned buckets for `avg`, `sum`,
`min`, `max`, and `count` through the extension's public
`timeless_window_batches` TVF. The TVF returns one `TWB1` bucket blob per
series; the reader pool keeps one prepared statement per connection and shapes
labels through the existing immutable ETS catalog cache.

Starting revisions were `d355d3d1ed43ad2f77e90e47b7a3455d57ba9d6b` in
TimelessMetrics and `a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69` in
timeless-libsql, both on their existing feature branches with the paired work
uncommitted.

## Command and environment

```sh
MIX_ENV=test mix run bench/engine_query_bench.exs --engine rust --runs 20
MIX_ENV=test mix run bench/engine_query_bench.exs --engine libsql --runs 20
```

Core Ultra 9 185H, 22 schedulers, `powersave`, `/tmp` on tmpfs, Elixir
1.20.2/OTP 29, SQLite 3.53.2. Dataset: one metric, 12,000 series, four labels,
60 points per series, 720,000 total points, and 72,000 ten-second buckets.
Engines ran in fresh processes sequentially.

## Side-by-side result

| Query | Rust median / p95 | libSQL median / p95 | Gap |
|---|---:|---:|---:|
| Exact raw, 60 points | 0.153 / 0.220ms | 0.328 / 0.541ms | 2.14x / 2.46x |
| Narrow raw, 188 series | 1.193 / 1.396ms | 3.494 / 4.485ms | 2.93x / 3.21x |
| Wide raw, 720K points | 43.131 / 51.486ms | 368.684 / 410.953ms | 8.55x / 7.98x |
| Scalar average, 12K values | 27.683 / 30.976ms | 41.036 / 43.960ms | 1.48x / 1.42x |
| Bucketed average, 72K values | 119.937 / 124.265ms | 127.532 / 135.006ms | 1.06x / 1.09x |
| Latest, 12K values | 52.730 / 64.257ms | 40.994 / 43.011ms | libSQL 1.29x / 1.49x faster |

The saved pre-Session-3 libSQL bucketed median/p95 was 495.455/506.141ms.
The final 127.532ms median is a 3.88x improvement
and clears the session's 3x gate. It is within 20% of Rust at both median and
p95 for this representative query.

## Attribution

The first correct row-oriented adapter measured 273.657ms. Its native fetch
alone was 259.461ms for 72K SQLite rows; BEAM shaping was only 8.935ms. That
evidence triggered the packed TVF rather than speculative format work.

Final fixed-reader medians:

| Stage | Median | Result |
|---|---:|---:|
| Row `timeless_window` fetch | 240.041ms | 72K rows |
| Row decode/shape | 8.737ms | 12K series / 72K values |
| Packed `timeless_window_batches` fetch | 96.516ms | 12K rows |
| Packed decode/shape | 6.114ms | 12K series / 72K values |
| Packed fetch + decode + shape | 126.756ms | 12K series / 72K values |

## Compatibility boundary

TimelessMetrics' inclusive integer-second bucket
`[bucket_start, bucket_start + step - 1]` maps to the native half-open window
`(t - step, t]` at `t = bucket_start + step - 1`. Native routing therefore
requires a positive step, a non-empty inclusive range whose span is exactly
divisible by the step, no more than one million grid points, and one of the
five proven aggregates.

Partial terminal buckets, `first`, `last`, and Timeless rate carry-in remain on
the raw path. Pointwise transforms run after native aggregation. Equality
labels push down; complex matchers retain the existing postfilter semantics.

Validation:

- focused libSQL tests: 9 passed
- full TimelessMetrics suite: 467 passed
- VictoriaMetrics differential: 182/182
- extension workspace and all 36 CLI sections passed, including the 150K-op
  oracle and five crash/reopen rounds
- packed sparse/dense blobs pinned byte-for-byte; core batch equals by-id oracle
