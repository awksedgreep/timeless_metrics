# Standalone query API adoption — 2026-08-01

Session 6 adopts the standalone `timeless-libsql` query surface in
TimelessMetrics without changing either public API. Cached exact reads use the
extension's `series_id` constraint, while wide scalar aggregates and latest
queries use the one-row TAF1/TLF1 transports. Row TVFs remain prepared and are
selected automatically when the frame modules are absent.

Starting revisions were `a359aa5` in TimelessMetrics and `650fe13` in
`timeless-libsql`. The packaged wrapper is now pinned to immutable extension
revision `6fc33006c1f3d6605b4feb76018ef6b21d17383b`.

## Controlled adapter comparison

Both runs used the same Session 6 TimelessMetrics code and machine. The only
change was the loaded extension: the prior row-only revision for the fallback
run and revision `6fc3300` for the packed run.

```sh
MIX_ENV=test mix run --no-compile bench/engine_query_bench.exs \
  --engine libsql --series 12000 --points 60 --runs 10
```

Environment: Core Ultra 9 185H, Linux, 22 schedulers, Elixir 1.20.2/OTP 29,
SQLite 3.53.2, `/tmp` on tmpfs. The dataset contains 12,000 series, 60 points
per series, and 720,000 total points.

| Public query | Row median / p95 | Packed median / p95 | Median change |
|---|---:|---:|---:|
| Scalar average, 12K series | 38.833 / 40.554ms | 12.695 / 14.691ms | 3.06x faster |
| Latest, 12K series | 37.488 / 39.717ms | 12.332 / 13.662ms | 3.04x faster |
| Exact raw, 60 points | n/a | 0.259 / 0.430ms | selected-ID route |
| Exact latest | n/a | 0.217 / 0.343ms | selected-ID route |

The packed public queries are faster than the prior row TVFs' prepared-fetch
stage alone: 36.115ms for aggregate rows and 37.841ms for latest rows in the
packed-run process. The direct-extension characterization separately measured
roughly 4.45ms for either frame at this cardinality; the remaining 8ms is NIF
decode, immutable-label lookup, and construction of the final public maps.

## Peak process memory

The benchmark samples the worker process, including referenced binaries, and
compares the peak increment to the serialized final result.

| Public query | Row peak increment | Packed peak increment | Packed/result multiple |
|---|---:|---:|---:|
| Scalar average | 13,326,552 bytes | 9,207,952 bytes | 5.730x |
| Latest | 8,235,544 bytes | 10,781,160 bytes | 6.514x |

TAF1 reduced the sampled scalar peak by 30.9%. TLF1 reduced SQLite transport
bytes but increased the sampled public-process peak by 30.9%; the final 12,000
maps and BEAM heap-growth thresholds dominate this measurement. The increase
is recorded rather than hidden because the latency win does not imply a memory
win for every result shape.

## Compatibility and semantic gates

- Frame availability is detected through `pragma_module_list`; no extension
  version string is parsed.
- TAF1/TLF1 have strict dirty-CPU NIF decoders. Unknown versions, bad lengths,
  flags, reserved bits, bitmap padding, noncanonical NULL words, invalid count
  NULLs, count overflow, and valid NaNs fail loudly.
- The frame and row routes are compared through the public aggregate/latest
  APIs in the focused suite, including residual matchers and integer counts.
- Cached exact raw, aggregate, and latest reads are pinned against a strict
  label superset so a selected ID cannot accidentally fan out.
- Older extensions do not push down visible `series_id` constraints. Session 6
  initially exposed the resulting 60-70ms exact-read scan in the controlled
  fallback run; selected-ID statements are now gated with the detected frame
  generation so old extensions retain their fast label-filter statements.
- Existing restart tests now exercise packed aggregate/latest reads after
  reopen. The writer barrier and prepared reader pool are unchanged.

The packed transports improve the TimelessMetrics boundary, but remain public,
documented extension features with independent Rust and Python decoders. No
PromQL or Elixir-specific result shape moved into SQLite.
