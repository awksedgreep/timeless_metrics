# Matcher and discovery pushdown through TimelessMetrics — 2026-07-31

This is the TimelessMetrics-side result for Session 7 of the extension-first
query plan.

## Boundary

Previously, the libSQL adapter sent only unique, non-empty equality matchers to
the extension. Regex, negative, empty-equality, and duplicate matchers were
applied after the query returned. A selective regex could therefore read,
transport, and decode all 720,000 points before Elixir retained one series.

The adapter now builds a hybrid plan:

- non-empty equality, empty equality, inequality, and a portable regex subset
  are sent to the public extension matcher JSON;
- duplicate matchers may contribute one necessary storage predicate, but the
  complete duplicate AND remains as an Elixir residual;
- PCRE-only or dialect-sensitive regexes remain entirely above the boundary;
- an invalid regex preserves the existing empty-result contract without
  executing a storage query.

The portable subset is deliberately conservative. Tests pin PCRE lookahead and
Unicode single-dot cases to the residual path rather than assuming equivalence
with Rust's RE2-family engine. HTTP series, label-name, and label-value
discovery now uses the same storage planner through `find_series/3`.

## Reproduction

Starting TimelessMetrics revision:
`d355d3d1ed43ad2f77e90e47b7a3455d57ba9d6b` on
`feat/libsql-storage-engine`, with the migration work uncommitted. The paired
extension revision was `a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69` on
`feat/timeless-metrics-embedding`, also dirty.

```sh
MIX_ENV=test mix run bench/engine_query_bench.exs --engine libsql --runs 5
MIX_ENV=test mix run bench/engine_query_bench.exs --engine rust --runs 5
```

The local validation invocation forced both paired NIFs to build from source.
Environment: Linux 7.1.3 x86-64, Intel Core Ultra 9 185H, 22 BEAM schedulers,
Elixir 1.20.2, OTP 29, bundled SQLite 3.53.2, and `/tmp` on `tmpfs`.

## Before and after

| libSQL public shape | Before median / p95 | After median / p95 | Change |
|---|---:|---:|---:|
| Selective regex raw, 1 series / 60 points | 116.944 / 130.709ms | 3.083 / 3.323ms | **37.93x faster** |
| Selective regex discovery, 1 label map | 52.427 / 56.889ms | 2.431 / 3.380ms | **21.57x faster** |

The baseline and optimized runs used the same benchmark code, dataset, process
shape, and five timed raw samples; discovery used 100 samples. The old
selective regex was slower than the wide read because it performed the wide
read plus post-filtering.

## Current Rust/libSQL comparison

| Shape | Rust median | libSQL median | Result |
|---|---:|---:|---:|
| Selective regex raw | 74.162ms | 3.083ms | libSQL 24.06x faster |
| Selective regex discovery | 23.756ms | 2.431ms | libSQL 9.77x faster |
| Negative equality raw, 6K series / 360K points | 69.629ms | 57.008ms | libSQL 1.22x faster |

The Rust block-engine adapter still evaluates complex matchers after its raw
NIF query, which explains its selective-regex result; this record does not
change that engine. Wide raw remains a separate Session 8 release-gate shape.

## Correctness coverage

The differential corpus compares storage results with
`TimelessMetrics.LabelMatch` for equality, empty equality, both negative
operators, absent labels, portable regexes, duplicate matchers, invalid
patterns, PCRE lookahead, and Unicode dot behavior. Raw, scalar aggregate,
bucketed aggregate, latest, direct discovery, and selector-scoped HTTP
discovery use the shared plan. Unsupported patterns retain the original raw
and post-filter fallback.
