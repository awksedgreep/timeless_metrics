# Metrics API POC Session 4 — first PromQL vertical slice

Date: 2026-08-01  
Branches: `poc/rust-telemetry-data-plane` in `timeless-libsql` and
`timeless_metrics`

## Outcome

Session 4 moves a deliberately narrow but realistic PromQL slice completely
behind the Rust HTTP boundary:

- instant and range vector selectors, including equality, inequality, regex,
  negative regex, missing-label, and duplicate matcher semantics;
- `avg_over_time(selector[window])` for instant and range requests;
- Prometheus vector/matrix success envelopes and explicit `bad_data` /
  `execution` error envelopes;
- both `/api/v1/query[_range]` and `/prometheus/api/v1/query[_range]` aliases,
  with GET query and POST form-body merging.

The parser is storage-independent and produces only the two supported plan
types. Plain selectors consume the public `timeless_raw_frame` surface and use
an allocation-free two-pointer sweep for the strict 300-second
`(T-lookback,T]` grid. `avg_over_time` lowers directly to the public
`timeless_window_batches` TVF. The API serializes final Prometheus JSON in
Rust; there is no BEAM/NIF data transport, hidden fallback, or new storage
mechanism.

## Correctness and cancellation

The extension-backed tests pin:

- strict selector staleness and strict range-window lower bounds;
- timestamps exactly on `start + n*step`;
- metric-name retention for both supported plans;
- duplicate matcher AND semantics and exact `__name__` selectors;
- numeric/float/RFC3339 timestamps and numeric/duration steps;
- instant `vector` versus range `matrix` response shapes;
- POST body precedence, explicit unsupported-expression errors, telemetry,
  flush, shutdown, and reopen.

A separate cancellation regression seeds 4,000 series, begins an 11,000-step
selector request on a one-reader pool, drops the HTTP future, and verifies one
cancelled request, zero read errors, zero API reads in flight, and a successful
fresh instant query on that same reader. Cancellation uses a per-request token,
a scoped SQLite progress handler, and checks between host grid points.

## Fixed black-box comparison

Both fresh servers were seeded independently with the identical fixed fixture:

- 200 devices x 20 metrics x 100 one-second samples;
- 4,000 series and exactly 400,000 points per server;
- fixed first timestamp `1700000000`;
- one writer, two readers, explicit flush, deferred scheduled maintenance;
- sequential socket-to-body requests from the same Req client process;
- 300 exact-shape iterations and 50 100-series iterations after 10 warmups.

Before timing, the harness decoded and compared every response from the Rust
server and Elixir+libSQL control. All 6/6 shapes matched, and response byte
counts were identical.

| Query shape | Response | Elixir median | Elixir p95 | Rust median | Rust p95 | Rust p95 verdict |
|---|---:|---:|---:|---:|---:|---:|
| selector exact, 100 steps | 2,155 B | 345 us | 784 us | 636 us | 1,115 us | 1.42x slower |
| selector 100 series, 100 steps | 224,058 B | 8,559 us | 10,709 us | 2,081 us | 4,019 us | 2.66x faster |
| `avg_over_time` exact, 100 steps | 3,028 B | 587 us | 869 us | 263 us | 600 us | 1.45x faster |
| `avg_over_time`, 100 series, 100 steps | 302,624 B | 9,946 us | 12,401 us | 2,327 us | 3,056 us | 4.06x faster |
| selector exact instant | 182 B | 166 us | 366 us | 459 us | 713 us | 1.95x slower |
| `avg_over_time` exact instant | 183 B | 167 us | 366 us | 403 us | 706 us | 1.93x slower |

The exact selector/instant shapes retain the small fixed Rust HTTP/catalog
cost already observed in Session 3, but remain below 1.12ms p95. The useful
fan-out cases are substantially faster because the final grid evaluation and
JSON encoding no longer build Elixir per-series/per-point object graphs.

After both fixtures, both differential passes, and both timing passes:

| Process | VmRSS | VmHWM |
|---|---:|---:|
| Elixir+libSQL | 204,784 KiB | 315,596 KiB |
| Rust API | 44,464 KiB | 58,272 KiB |

Rust used 81.5% less peak resident memory (5.42x smaller HWM) in this
same-lifecycle comparison.

## Reproduction

Build the extension and Session 4 server, then start fresh Rust and
Elixir+libSQL processes on separate ports. Seed each with the same timestamp:

```bash
mix run --no-start --no-compile bench/metrics_api_seed.exs -- \
  --url http://127.0.0.1:PORT --devices 200 --metrics 20 --samples 100 \
  --first-timestamp 1700000000
```

Run the differential and one side's timings (reverse the URLs for the control):

```bash
mix run --no-start --no-compile bench/metrics_api_promql.exs -- \
  --url http://127.0.0.1:RUST_PORT \
  --reference-url http://127.0.0.1:ELIXIR_PORT \
  --runs 300 --wide-runs 50
```

## Verdict

Session 4 meets its exit criterion. One real PromQL selector/window slice now
runs socket-to-response in Rust, matches the established Elixir+libSQL wire
contract, is cancellable, and uses only public extension primitives available
to direct SQLite/libSQL users. The intentionally small grammar remains honest:
unsupported PromQL is visible work for later sessions rather than a silent
cross-process fallback.
