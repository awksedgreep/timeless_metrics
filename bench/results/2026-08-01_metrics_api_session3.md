# Rust metrics API POC — Session 3 mechanical reads

Session 3 moves native latest, range/export, and discovery across the process
boundary without moving storage out of the existing `timeless_metrics`
extension. PromQL is not part of this session; unsupported `query=` requests
return an explicit `400` instead of silently crossing into Elixir or changing
semantics.

## Implemented boundary

- `GET|POST /api/v1/query` serves native exact latest through
  `timeless_latest_frame`, with `timeless_latest` row fallback.
- `GET /api/v1/export` serves inclusive VictoriaMetrics JSON-line raw export
  through `timeless_raw_frame`, with `timeless_raw` row fallback.
- `GET|POST /api/v1/query_range` uses `timeless_window_batches` for complete
  avg/sum/min/max/count grids. Partial grids and first/last/rate retain the
  established raw aggregation behavior over `timeless_raw_frame`.
- Label names/values and native/Prometheus series discovery use public
  `timeless_series` and `timeless_label_values` TVFs.
- Repeated `match[]`/`match` selectors are a union. Matchers within one
  selector are ANDed, duplicate label keys are preserved, regexes are fully
  anchored, and absent labels match as the empty string.
- Capability discovery queries `pragma_module_list`. It does not compare or
  parse an extension version string.

`TLF1`, `TRF1`, and `TWB1` envelopes are checked for version, exact length,
counts, bitmap padding, validity, and NaN/null invariants before encoding a
response. The raw and window fast paths retain one SQLite blob and borrow its
column offsets while writing final JSON; they do not allocate parallel
timestamp and value vectors. Per-series metadata remains bounded by the
catalog/result cardinality.

## Compatibility contract

The release-extension `session_three_pins_mechanical_reads_discovery_and_reopen`
fixture proves:

- query-string and POST-form merging with body values taking precedence;
- exact single/multi latest envelopes and deterministic series ordering;
- inclusive export/range edges, no trailing export newline, and an empty `200`
  export body;
- native packed-window avg plus a non-divisible raw-frame avg fallback;
- native series wrappers versus Prometheus label maps containing `__name__`;
- metric equality/regex, duplicate matchers, repeated selector union, and
  missing-label-as-empty behavior;
- malformed selector and explicit Session 4 PromQL rejection;
- cumulative read requests, frame/response bytes, returned series/points, and
  zero execution errors; and
- identical latest data after graceful shutdown and reopen.

The matcher fixture caught a test-expectation error during validation:
`env!="prod"` must include a series with no `env`, since the waist contract
compares that absent label as `""`. The implementation was correct and the
regression now pins it.

## Fixed mechanical comparison

### Method

- Intel Core Ultra 9 185H, Linux 7.1.3, Elixir 1.20.2 / OTP 29.
- Separate fresh Elixir+libSQL and Rust+libSQL processes, two SQLite readers,
  deferred scheduled maintenance.
- `metrics_api_seed.exs`: exactly 200 devices x 20 metrics x 100 samples =
  4,000 series and 400,000 points, then an explicit flush.
- `metrics_api_reads.exs`: 500 socket-to-body iterations for latest/range/
  export and 50 for each discovery shape, after ten warmups per shape.
- Range/export bounds are anchored to the fixture's exact latest timestamp.
  The inclusive range contains 3,600 seconds, divisible by the 15-second step,
  so the common range uses `TWB1` in both implementations.
- Response byte counts must match before timings are accepted.
- HWM is `/proc/<pid>/status` `VmHWM` after seed, flush, warmups, and reads in
  the same process lifecycle.

### Accepted result

| shape | response bytes | Elixir median / p95 | Rust median / p95 | Rust vs control p95 |
|---|---:|---:|---:|---:|
| exact latest | 103 | 143 / 272us | 297 / 651us | 2.39x slower |
| exact 15s range | 283 | 161 / 310us | 262 / 561us | 1.81x slower |
| exact raw export | 2,007 | 151 / 248us | 417 / 752us | 3.03x slower |
| all label names | 62 | 63.71 / 70.35ms | 9.01 / 10.54ms | 6.68x faster |
| metric label values | 3,229 | 552 / 722us | 475 / 672us | 1.07x faster |
| metric series | 13,979 | 3.70 / 5.15ms | 758us / 1.06ms | 4.88x faster |
| exact selector series | 8,129 | 934us / 1.08ms | 751 / 957us | 1.13x faster |

Same-lifecycle process memory:

| process | VmHWM | VmRSS after run |
|---|---:|---:|
| Elixir HTTP + libSQL | 244,796KiB | 223,716KiB |
| Rust API + libSQL | **56,948KiB** | **42,268KiB** |

Rust HWM is 76.7% lower. Its three exact data paths retain a roughly
0.25-0.40ms median API/serialization tax relative to the in-process Elixir
control, but all remain below 0.8ms at p95. Catalog discovery removes enough
Elixir/host work to more than offset the Rust HTTP tax.

The final Rust process reported 1,771 read requests, zero errors, 908,853
packed frame bytes, 2,744,473 response bytes, 259,531 returned series, 55,081
returned points, and 881.7ms cumulative request-to-result time (498us/request
including discovery).

### Discarded setup rows

Two exploratory result sets are intentionally excluded:

1. The first script used a five-minute wall-clock lookback. The Elixir process
   was reopened nine minutes after its mixed load, so range/export were empty.
2. The first one-hour correction still anchored buckets to each benchmark's
   wall clock. Separately seeded fixtures therefore had different 15-second
   phases and different range response cardinality.

The final harness derives its stop bound from exact latest. Accepted rows have
identical response byte counts for all seven shapes.

## Mixed native read/write result

`realistic_workload.exs` gained `--query-format native`; its default remains
`promql`. Native mode sends the same exact `metric`/`host` latest and range
requests to both servers, avoiding a false claim that Session 3 implements the
Session 4 PromQL slice.

The method otherwise matches Session 2: 4,000 series, four writers, 1,000
points/request, two query workers, deterministic seed 42001, six three-second
steps through a 3ms writer interval, scheduled maintenance deferred, and final
ordered drain.

| measurement at final step | Elixir control 1 | Elixir control 2 | Rust API |
|---|---:|---:|---:|
| completed points/s | 782.5K | 604.8K | **866.6K** |
| admitted points/s | 786.5K | 868.4K | **870.3K** |
| write p95 | 912us | 916us | **416us** |
| query/s | 91 | 64 | **167** |
| query p95 | 70.26ms | 93.71ms | **10.08ms** |
| query p99 | 134.28ms | 910.25ms | **19.40ms** |
| final queue / age | 4 / 4ms | 381 / 487ms | 8 / 10ms |
| final drain | 813.23ms | **403.13ms** | 723.35ms |
| VmHWM | 460,152KiB | 457,128KiB | **180,716KiB** |

The Rust run completed 10.7% more work than the stronger Elixir control, with
6.97x lower query p95 and 60.7% lower HWM. Against the second control it was
43.3% faster with 9.30x lower query p95. It admitted/completed every request,
reported no read/write error, and drained to zero. Its post-run query telemetry
reported 4,081 requests, 25,253,869 packed frame bytes, only 684,659 response
bytes, 4,053 returned series, and 7,320 returned points.

Both Elixir controls logged the same pre-existing
`cached_labels_by_sid/2` empty-match crash from the raw-frame label cache while
their measured client windows reported zero errors. One run logged one crash;
the repeat logged three. The repeat also accumulated a 381-request writer
queue. These are control-boundary observations, not attributed to the storage
extension or hidden as Rust wins. The fixed sequential comparison above is the
clean route-level comparison; the mixed test shows the operational consequence
of removing that mutable Elixir cache/boundary from API reads.

## Validation

```text
cargo fmt --all -- --check                         passed
timeless-libsql cargo test --workspace             passed
metrics API strict Clippy                          passed
metrics API unit/doc tests                         passed
release-extension storage_contract                3 passed
tests/correctness.sh r1                            passed
fixed response-byte parity                         7/7 shapes
mixed Rust read/write errors                       0
mixed Rust final queue/in-flight                   0/0 after drain
mix format --check-formatted                       passed
MIX_ENV=test mix test                              482 passed
```

## Verdict

Session 3 meets its exit criterion. Exact data reads pay a small, bounded Rust
HTTP/serializer cost and are not presented as a universal latency win. The
process becomes dramatically smaller, discovery becomes substantially faster,
mixed query tails fall by roughly an order of magnitude, and writes remain
healthy. Every accelerator used by the server is still a public extension TVF
available to direct SQLite/libSQL users; the API added planning and final wire
encoding, not a second metrics engine.

Session 4 can now implement one explicit PromQL vertical slice without
reopening storage, ingest, matcher, frame, discovery, or native response work.

## Reproduction

Seed each fresh server:

```bash
mix run --no-start --no-compile bench/metrics_api_seed.exs -- \
  --url http://127.0.0.1:PORT --devices 200 --metrics 20 --samples 100
```

Run the accepted mechanical comparison:

```bash
mix run --no-start --no-compile bench/metrics_api_reads.exs -- \
  --url http://127.0.0.1:PORT --runs 500 --discovery-runs 50 \
  --lookback-seconds 3600
```

Run mixed native reads/writes:

```bash
mix run --no-start --no-compile bench/realistic_workload.exs -- \
  --tm-url http://127.0.0.1:PORT --vm-url '' \
  --devices 200 --metrics 20 --batch 50 \
  --step-seconds 3 --settle-seconds 1 \
  --steps 0.1,0.05,0.025,0.0125,0.00625,0.003125 \
  --query-workers 2 --warmup 1 --seed 42001 \
  --write-format prometheus --query-format native
```
