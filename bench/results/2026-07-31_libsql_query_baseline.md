# Rust/libSQL public query baseline — 2026-07-31

This is the first TimelessMetrics-side Session 0 result for the extension-first
read/query performance plan. Both engines use the same deterministic public-API
workload and run in separate BEAM processes.

## Reproduction

Starting TimelessMetrics revision:
`d355d3d1ed43ad2f77e90e47b7a3455d57ba9d6b` on
`feat/libsql-storage-engine`, with the libSQL engine work uncommitted. Starting
extension revision: `a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69` on
`feat/timeless-metrics-embedding`, also dirty.

```sh
MIX_ENV=test mix run bench/engine_query_bench.exs --engine rust --runs 10
MIX_ENV=test mix run bench/engine_query_bench.exs --engine libsql --runs 10
```

Environment:

- Linux 7.1.3, x86-64; Intel Core Ultra 9 185H; 22 BEAM schedulers
- CPU governor: `powersave`
- data directories: `/tmp` on `tmpfs`
- Elixir 1.20.2, OTP 29; libSQL's bundled SQLite 3.53.2
- one metric, four labels per series, 12,000 series × 60 points = 720,000 points
- engines run sequentially, never concurrently

## Side-by-side result

These are the medians from the final sequential pair. P95 is included because
the governor and hybrid CPU caused visible tail variance.

| Shape | Result | Rust median / p95 | libSQL median / p95 | Median gap |
|---|---:|---:|---:|---:|
| First exact after flush | 60 points | 1.164 / 1.164ms | 54.818 / 54.818ms | 47.09x |
| Exact raw | 60 points | 0.130 / 0.182ms | 0.287 / 0.422ms | 2.21x |
| Narrow raw | 188 series, 11,280 points | 1.041 / 1.164ms | 3.728 / 5.965ms | 3.58x |
| Wide raw | 12K series, 720K points | 43.651 / 80.911ms | 372.787 / 390.146ms | 8.54x |
| Scalar average | 12K values | 28.852 / 33.126ms | 449.270 / 478.265ms | 15.57x |
| Bucketed 10-second average | 72K values | 115.681 / 136.101ms | 495.455 / 506.141ms | 4.28x |
| Latest multi | 12K points | 50.962 / 78.787ms | 428.097 / 436.712ms | 8.40x |

The externally encoded wide result is 12,959,022 bytes for both engines. Store
bytes at measurement time were 3,850,763 for Rust and 7,104,624 for libSQL;
these are workload-local figures, not the 10M-point storage benchmark.

## libSQL attribution

| Named stage | Median | P95 | Share/interpretation |
|---|---:|---:|---|
| Public wide raw | 372.787ms | 390.146ms | end to end |
| Combined fixed-reader fetch + decode + shape | 373.073ms | 388.980ms | 100.1% of public median |
| Prepared Exqlite/SQLite packed-row fetch alone | 165.635ms | 172.995ms | 44.4% when isolated |
| Packed labels/points decode alone | 93.718ms | 129.214ms | isolated; allocation/GC is nonlinear |
| Sort/shape alone | 5.696ms | 8.203ms | 1.5% |
| No-pending writer barrier | 0.009ms | 0.017ms | negligible warm cost |
| Scalar reduction after decode | 16.799ms | 18.297ms | arithmetic is not the bottleneck |

The isolated stages are not additive: fetching followed immediately by eager
decode keeps the packed blobs alive while allocating 720,000 point tuples and
provokes much more GC than either isolated stage. The combined measurement
reproduces the public wide query within noise, satisfying the initial wall-time
attribution requirement. The direct extension performs the comparable packed
wide scan in 99.975ms median, which also exposes a meaningful SQLite/Exqlite
and BEAM materialization floor.

## Fresh-process variance and decision

| Engine/shape | Recorded process medians | Range |
|---|---|---:|
| Direct extension wide | 109.319, 108.028, 99.975ms | 9.344ms |
| Public libSQL wide | 373.479, 370.490, 372.787ms | 2.989ms |
| Public libSQL scalar | 426.530, 455.424, 449.270ms | 28.894ms |
| Public Rust wide | 71.483, 67.086, 43.651ms | 27.832ms |
| Public Rust scalar | 27.181, 26.663, 28.852ms | 2.189ms |

The Rust wide result is not stable enough for a release claim under the current
`powersave` governor. A controlled-governor run remains a Session 0 gate.
However, the attribution is stable enough to choose the next implementation:
native scalar aggregate and latest TVFs should return 12K compact rows instead
of transporting and materializing 720K raw points. Wide raw transport remains
important, but its eager public result is inherently allocation-heavy.
