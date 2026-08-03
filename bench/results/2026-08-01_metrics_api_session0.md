# Rust metrics API POC — Session 0 control baseline

Session 0 pins the current Elixir HTTP control before any Rust API server is
written. The primary control is `TimelessMetrics.HTTP` over `engine: :libsql`;
the established Rust block engine is the secondary product comparison.

## Reproducible contract

`test/data_plane_contract_test.exs` now runs the same black-box fixture against
both engines and fixes these current behaviors:

- VictoriaMetrics JSON-line and Prometheus text imports return an asynchronous
  empty `204` after queue admission.
- A partially valid body persists its valid samples and skips malformed lines.
  The fixture admits two requests, completes two requests and four valid points,
  and reports four rejected lines: malformed JSON, Prometheus `NaN`, `+Inf`, and
  a malformed Prometheus sample.
- Both import formats interpret explicit timestamps as milliseconds and divide
  them by 1,000 for storage. The existing no-timestamp Prometheus fixture pins
  current-time behavior.
- Repeated lines for the same metric/label set remain one series, with all valid
  timestamp/value pairs queryable in order.
- Native latest, one-second range, Victoria export, label names/values, and
  series discovery return the pinned response envelopes on both engines.
- Range/export endpoints include points exactly on both requested time edges.
- A missing export returns HTTP 200 with an empty body.
- `POST /api/v1/flush` is an authenticated operational barrier: it waits for
  all admitted HTTP batches to finish, flushes the storage engine, and returns
  admitted/completed batch and completed-point counters.

`GET /health` gained additive fields without changing the existing fields:
`completed_points`, `admitted_batches`, `completed_batches`, `queued_batches`,
`in_flight_batches`, `oldest_queued_ms`, and `import_errors`. Async parser errors
were previously discarded; they are now counted while partial-acceptance and
HTTP response semantics remain unchanged.

## Workload contract

`bench/realistic_workload.exs` now:

- accepts `--seed` and seeds each writer/query process independently;
- correctly creates zero query tasks for `--query-workers 0`;
- distinguishes offered, HTTP-admitted, and storage-completed points;
- uses completed points—not `204` admission—for saturation and peak throughput;
- samples queue depth, in-flight batches, and oldest queued age per step;
- reports write/query p50, p95, and p99;
- stops writers before invoking the explicit drain-and-flush barrier; and
- no longer double-counts `points + buffer_points` in final verification.

The client and server must use `--no-compile` when run concurrently. Rebuilding
and copying a loaded NIF shared object from the client process can invalidate
the server mapping; one discarded setup attempt exposed exactly that harness
hazard. No number from failed setup attempts appears below.

## Environment and method

- Host: Intel Core Ultra 9 185H, 22 logical CPUs, Linux 7.1.3
- Runtime: Elixir 1.20.2, Erlang/OTP 29
- Metrics control commit before Session 0 changes: `2cd785f`
- Extension branch commit: `b582a43`
- Fresh server process and fresh data directory for every row
- 4,000 series: 200 devices × 20 metrics
- Four writers, 50 devices/request, 1,000 points/request
- Six fixed three-second measured steps at 100/50/25/12/6/3ms writer intervals
- One-second warmup and one-second settle per step; deterministic seed 42001
- Two libSQL readers, four HTTP ingest workers
- Rollup and retention timers moved to 24 hours; normal buffering, automatic
  storage flush behavior, and the final explicit flush remained enabled
- HWM is the server process's Linux `/proc/<pid>/status` `VmHWM`; the client ran
  in a separate process

The final 3ms row is below the requested 1.33M points/s because four sequential
writers become the client/HTTP pacing limit around 0.8M points/s. The small
cases where a measured completed rate exceeds admitted rate are batches crossing
the adjacent sampling boundary. Final drain proves every run reaches queue zero.

## Final-step comparison

| Engine | query workers | completed points/s | write p95 | write p99 | query/s | query p95 | query p99 | final queue | drain | HWM |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Elixir + libSQL | 0 | 779.9K | 919µs | 1.12ms | — | — | — | 0 | **481.72ms** | 376,672KiB |
| Rust block | 0 | **811.8K** | **743µs** | **886µs** | — | — | — | 0 | 710.66ms | **273,908KiB** |
| Elixir + libSQL | 1 | 792.7K | 947µs | 1.17ms | 57 | 49.98ms | 87.38ms | 0 | **552.90ms** | 396,808KiB |
| Rust block | 1 | **819.1K** | **709µs** | **862µs** | **107** | **1.31ms** | **1.56ms** | 0 | 694.28ms | **282,208KiB** |
| Elixir + libSQL | 2 | 777.0K | 939µs | 1.14ms | 84 | 80.39ms | 139.61ms | 0 | **591.67ms** | 390,472KiB |
| Rust block | 2 | **806.6K** | **765µs** | **985µs** | **213** | **1.22ms** | **1.73ms** | 0 | 703.51ms | **290,452KiB** |

All six runs had zero HTTP write errors and zero query errors. At this workload,
libSQL completed within 3.2–3.9% of the Rust block engine. The read-side control
gap is intentionally visible: at the final mixed steps, Rust delivered 1.9–2.5×
the query throughput and libSQL's query p95 was 38–66× higher. The future Rust
API POC must compare to Elixir+libSQL first so this storage-engine difference is
not misattributed to the process boundary.

## Final storage state

| Engine | query workers | points | logical storage | data-directory bytes | bytes/point |
|---|---:|---:|---:|---:|---:|
| Elixir + libSQL | 0 | 7,362,000 | 11,175,558 | 15,915,720 | 1.518 |
| Elixir + libSQL | 1 | 7,386,000 | 11,234,644 | 15,981,352 | 1.521 |
| Elixir + libSQL | 2 | 7,389,000 | 11,228,868 | 16,014,168 | 1.520 |
| Rust block | 0 | 7,515,000 | 10,595,139 | 11,666,255 | 1.410 |
| Rust block | 1 | 7,550,000 | 10,711,689 | 11,782,805 | 1.419 |
| Rust block | 2 | 7,498,000 | 10,596,169 | 11,667,285 | 1.413 |

The point totals differ slightly because each concurrent three-second step ends
on a wall-clock boundary. Every server reported exactly 4,000 series, zero
buffered points after the barrier, and no scheduled rollup/retention activity.

## Reproduction

Compile once before starting concurrent processes:

```bash
mix compile
```

Start one fresh control server (change engine, port, and data directory per
run):

```bash
mix run --no-start --no-compile bench/http_baseline_server.exs -- \
  --engine libsql --port 19540 --data-dir /tmp/tm-metrics-control \
  --readers 2 --ingest-workers 4 --maintenance deferred
```

Run the deterministic client from another shell:

```bash
mix run --no-start --no-compile bench/realistic_workload.exs -- \
  --tm-url http://127.0.0.1:19540 --vm-url '' \
  --devices 200 --metrics 20 --batch 50 \
  --step-seconds 3 --settle-seconds 1 \
  --steps 0.1,0.05,0.025,0.0125,0.00625,0.003125 \
  --query-workers 2 --warmup 1 --seed 42001
```

## Session 0 verdict

The control is reproducible, storage completion is no longer conflated with
HTTP admission, and the current compatibility boundary is executable on both
engines. Session 1 can now build the descriptive Rust server shell against the
existing libSQL extension without inventing metrics storage or hiding an
unmeasured baseline.
