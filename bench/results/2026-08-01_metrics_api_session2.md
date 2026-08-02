# Rust metrics API POC — Session 2 native batched ingest

Session 2 adds the two established metrics import contracts to the standalone
Rust process without changing the storage boundary. The server still reaches
metrics storage only through the public `timeless_metrics` extension surface.

## Implemented boundary

- `POST /api/v1/import/prometheus` retains the Axum body as reference-counted
  bytes and gives the complete exposition to the extension. The API neither
  parses nor copies the samples and issues one SQLite statement per request.
- `POST /api/v1/import` parses VictoriaMetrics JSON lines once, interns unique
  series within the request, normalizes millisecond timestamps, and encodes one
  public named-columnar `0x01` batch. It also issues one statement per request.
- Both routes return the established empty asynchronous `204` after bounded
  writer admission. Partially valid bodies retain valid samples and count bad
  lines. All-invalid bodies complete as zero-point requests rather than
  becoming synchronous HTTP errors.
- The 10 MiB body limit rejects before parsing or admission. The queue is
  bounded by request batches and reports body bytes as well as known/unknown
  point counts.
- There is no per-point SQL, request compression, cross-request grouping,
  request-boundary storage flush, auth, query route, or host storage engine.

The VictoriaMetrics route deliberately uses named batch `0x01`. A durable
series-id cache and resolved batch `0x02` remain a later measured optimization;
they are not required for Session 2 correctness.

## Reusable extension telemetry

`timeless-core` now counts cumulative Prometheus ingest batches, valid points,
rejected lines, and fused parse/resolve/buffer nanoseconds.
`timeless_stats('metrics')`
publishes those fields, so direct SQLite/libSQL applications receive the same
visibility as the POC server.

The API combines extension Prometheus errors with its own VictoriaMetrics
parser errors. It separately reports request format, admitted/completed body
bytes, queue wait, SQLite statement time, and VictoriaMetrics parse/encode
time. Prometheus fused ingest time remains correctly attributed to the
extension.

## Compatibility contract

The extension-backed Session 2 test sends:

1. VictoriaMetrics JSON lines containing three valid points and one malformed
   line;
2. Prometheus text containing one valid point, `NaN`, `+Inf`, and one malformed
   line;
3. one all-malformed VictoriaMetrics request and one two-line all-malformed
   Prometheus request;
4. one Prometheus request beginning with reserved batch-version byte `0x01`;
   and
5. a 10 MiB + 1 byte body.

The first five requests all return empty `204`; the oversized request returns
`413` and is never admitted. Ordered flush reports five completed batches and
four completed points with no failed, queued, or in-flight work. Health reports
two series and eight rejected inputs. Direct persisted-row inspection proves
the four exact timestamps/values, and reopen recovers the same two series/four
points. The reserved byte is counted as malformed at the HTTP boundary and
cannot enter the extension's named-batch protocol. Extension telemetry
independently reports the two bodies actually parsed as Prometheus, one valid
Prometheus point, and five Prometheus parser errors.

## Benchmark method

The method matches the Session 0 no-query control:

- Intel Core Ultra 9 185H host, Linux 7.1.3;
- fresh release server, database, and process for every run;
- 4,000 series: 200 devices x 20 metrics;
- four writers, 50 devices/request, 1,000 points/request;
- six fixed three-second steps at 100/50/25/12/6/3ms writer intervals;
- one-second warmup and settle, deterministic seed 42001;
- one SQLite writer, two readers, 256-request queue;
- scheduled flush/compact/retention moved to 24 hours; final explicit flush
  retained; and
- server `VmHWM` read from `/proc/<pid>/status` after drain.

`realistic_workload.exs` now accepts `--write-format prometheus|victoria` and
generates the corresponding native body. The default remains Prometheus. As in
Session 0, the four sequential writers become the HTTP client pacing limit near
0.8M points/s; the Prometheus final step is therefore a demonstrated clean rate,
not a measured server saturation ceiling.

## Prometheus result versus the control

| measurement | Elixir + libSQL control | Rust API run 1 | Rust API run 2 |
|---|---:|---:|---:|
| final completed points/s | 779.9K | 855.6K | 855.2K |
| write p50 | — | 302us | 304us |
| write p95 | 919us | 448us | 448us |
| write p99 | 1.12ms | 556us | 556us |
| final queue depth / age | 0 / 0ms | 6 / 7ms | 9 / 9ms |
| final drain | 481.72ms | 744.67ms | 725.32ms |
| HTTP/storage errors | 0 | 0 | 0 |
| process HWM | 376,672KiB | 180,016KiB | 178,888KiB |
| final points | 7,362,000 | 7,714,000 | 7,777,000 |

The Rust API completes 9.7% more points/s than the Elixir+libSQL control at the
same final client step. Write p95 is 51% lower and HWM is 52% lower. Explicit
flush is 1.51-1.55x slower, but both runs drain all queued/in-flight work and
verify exactly 4,000 series with no errors.

## VictoriaMetrics named-batch result

| measurement | run 1 | run 2 |
|---|---:|---:|
| final completed points/s | 620.9K | 613.2K |
| write p50 | 1.67ms | 1.74ms |
| write p95 | 2.78ms | 2.87ms |
| write p99 | 3.51ms | 3.48ms |
| final queue depth / age | 4 / 7ms | 5 / 8ms |
| final drain | 738.25ms | 666.93ms |
| HTTP/storage errors | 0 | 0 |
| process HWM | 179,460KiB | 178,164KiB |
| final points | 6,243,000 | 6,252,000 |

The larger JSON-line payload and host parse/encode work reduce the client/server
rate relative to Prometheus, but the path remains bounded, stable across fresh
processes, and well above 600K completed points/s without resolved-series
caching. Queue depth remains single digit and the final barrier drains to zero.

## Phase attribution

All values below are cumulative after the final drain. SQLite statement time
includes extension ingest work; Prometheus extension time is therefore a
component of, not additional to, its SQLite value.

| format/run | points | parse/fused-ingest ns/point | encode ns/point | SQLite ns/point | average queue wait/request | max queue wait |
|---|---:|---:|---:|---:|---:|---:|
| Prometheus 1 | 7,714,000 | 767 | — | 1,011 | 454us | 10.49ms |
| Prometheus 2 | 7,777,000 | 663 | — | 889 | 390us | 11.21ms |
| VictoriaMetrics 1 | 6,243,000 | 1,108 | 248 | 1,477 | 2.13ms | 26.26ms |
| VictoriaMetrics 2 | 6,252,000 | 1,085 | 245 | 1,357 | 824us | 14.44ms |

Prometheus admission-lock wait averaged less than 0.51us/request and
VictoriaMetrics less than 0.62us/request. The queue is writer-service time, not
admission-lock contention. Final logical storage was 1.365-1.366 bytes/point for
Prometheus and 1.386-1.387 bytes/point for VictoriaMetrics.

## Profiling regression caught during validation

The first Prometheus load run completed only 179.0K points/s and filled the
256-request queue. Extension telemetry showed fused parse/resolve/buffer work
used just 2.57 seconds over 3.143 million points, while the API reported 17.60
seconds in its statement phase. The API had queried the full `timeless_stats`
TVF before and after every insert solely to derive point/error deltas.

That accounting was unnecessary. The virtual-table contract already returns
the accepted point count through SQLite `last_insert_rowid`, and extension
stats already own cumulative malformed-line totals. Removing both hot-path
stats scans preserved exact flush/error accounting and raised repeatable
completion to 855.2-855.6K points/s. The contract test now proves point counts
from the write result and error counts from post-completion extension telemetry,
so observability no longer requires per-request scans.

## Validation

```text
cargo fmt --all -- --check                         passed
cargo test --workspace                             passed
cargo test + strict Clippy (metrics API crate)     passed
release-extension storage_contract                2 passed
tests/correctness.sh r1                            passed
mix format --check-formatted benchmark             passed
mix test test/data_plane_contract_test.exs          3 passed
```

Workspace-wide Rust 1.97 strict Clippy still reports pre-existing style lints
in `timeless-codec`/`timeless-core`; the Session 2 API crate itself passes
`cargo clippy --all-targets -- -D warnings`.

## Verdict

Session 2 meets its exit criterion: both native formats have pinned partial
success semantics, exact flush/reopen results, bounded admission and bodies,
one statement per request, and no host storage implementation. Prometheus
already exceeds the Elixir+libSQL write control with about half the memory.
VictoriaMetrics establishes an honest named-series floor and a concrete later
case for measuring resolved batch `0x02`. Session 3 can proceed to mechanical
read and discovery routes without revisiting this storage boundary.

## Reproduction

Start a fresh release API server with scheduled maintenance deferred:

```bash
TIMELESS_METRICS_FLUSH_INTERVAL_SECS=86400 \
TIMELESS_METRICS_COMPACT_INTERVAL_SECS=86400 \
TIMELESS_METRICS_RETENTION_INTERVAL_SECS=86400 \
  poc/timeless-metrics-api/target/release/timeless-metrics-api \
  target/release/libtimeless_ext.so /tmp/metrics-session2.db 127.0.0.1:19560
```

From `timeless_metrics`, run either native format:

```bash
mix run --no-start --no-compile bench/realistic_workload.exs -- \
  --tm-url http://127.0.0.1:19560 --vm-url '' \
  --devices 200 --metrics 20 --batch 50 \
  --step-seconds 3 --settle-seconds 1 \
  --steps 0.1,0.05,0.025,0.0125,0.00625,0.003125 \
  --query-workers 0 --warmup 1 --seed 42001 \
  --write-format prometheus
```

Change the last flag to `--write-format victoria` for the JSON-line path.
