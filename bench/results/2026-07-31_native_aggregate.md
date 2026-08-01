# Native libSQL scalar aggregate result — 2026-07-31

TimelessMetrics now routes unbucketed `avg`, `sum`, `min`, `max`, and `count`
queries to the extension's public `timeless_aggregate` TVF. Bucketed,
`first`/`last`/`rate`, and other unsupported shapes retain raw fallback.

Starting revisions were
`d355d3d1ed43ad2f77e90e47b7a3455d57ba9d6b` in TimelessMetrics and
`a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69` in timeless-libsql, both on
their existing feature branches with the paired work uncommitted.

## Command and environment

```sh
MIX_ENV=test mix run bench/engine_query_bench.exs --engine rust --runs 10
MIX_ENV=test mix run bench/engine_query_bench.exs --engine libsql --runs 10
```

Core Ultra 9 185H, 22 schedulers, `powersave`, `/tmp` on tmpfs, Elixir
1.20.2/OTP 29, SQLite 3.53.2. Dataset: one metric, 12,000 series, 60 points
per series, 720,000 total points. Engines and fresh processes ran sequentially.

## Result

Pre-change libSQL scalar median was 449.270ms. Three finalized libSQL
processes produced:

| Process | Median | P95 | Native fetch + shape |
|---|---:|---:|---:|
| 1 | 35.142ms | 37.096ms | 35.487ms |
| 2 | 34.659ms | 37.584ms | 33.566ms |
| 3 | 36.558ms | 39.682ms | 32.806ms |

That is a 12.29-12.96x public-API improvement. The matching Rust scalar
median was 26.346ms, putting libSQL at 1.32-1.39x Rust and inside Session 1's
2x gate. The direct extension kernel measured 14.133-16.180ms.

The final three exact-raw medians were 0.276ms, 0.277ms, and 0.269ms versus
the 0.287ms baseline, so the aggregate route did not regress exact reads. Wide
raw also improved to 321.559-334.088ms in the finalized processes because the
immutable series-label cache lookup was hoisted out of the per-row cache-ref
validation path; wide transport remains a later session.

## Adapter boundary

- One aggregate statement is prepared per pooled reader.
- The internal projection returns only `series_id,value`; immutable labels are
  served from ETS, with one catalog hydration after restart.
- Reader selection is caller-sticky for prepared-statement/cache locality;
  independent caller processes still distribute across the pool.
- The measured aggregate fetch is roughly 30-32ms and Elixir shaping roughly
  3ms. Combined attribution matches the public API within process variance.
- A stale-ETS race after writer restart was fixed by validating the published
  cache reference and synchronizing with the replacement writer when needed.

Validation:

- focused libSQL tests: 7 passed
- full TimelessMetrics suite: 465 passed
- extension workspace and full direct CLI suite: passed
- formatting and `git diff --check`: passed
