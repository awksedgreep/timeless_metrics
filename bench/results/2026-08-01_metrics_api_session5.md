# Metrics API POC Session 5: Elixir control-plane boundary

Session 5 proves the proposed Phoenix/Rust process boundary in the real Canvas
query seam. It adds no storage implementation, auth route, or product-state
migration.

## Implemented boundary

`timeless_ui` now has three opt-in modules on
`poc/rust-telemetry-data-plane`:

- `TimelessUI.MetricsDataPlane.Process` starts `timeless-metrics-api` through
  an Erlang port, tracks the Rust OS pid and listener readiness, and lets OTP
  restart the child after abnormal exit.
- `TimelessUI.MetricsDataPlane.Client` crosses loopback HTTP once per complete
  operation. It owns no SQLite/libSQL connection and decodes a response only
  after Req receives the complete body. One invalid NDJSON line rejects the
  complete result.
- `TimelessUI.MetricsDataPlane.CanvasSource` switches only Canvas
  `metric_range/5` behind `source: :data_plane`. Status, subscriptions,
  current values, metadata, time range, and other product callbacks continue
  through the configured Elixir fallback source.

The application child is disabled by default. Enabling it requires explicit
binary, extension, database, and IPv4 loopback-listener paths. The Rust child
remains the sole telemetry-database owner. Phoenix sessions, authorization
policy, dashboards, poller/scrape configuration, alerts, annotations, and
cluster administration were not moved.

## Correctness and isolation gates

The focused UI suite runs eight tests and pins:

- identical `{:ok, [{timestamp_ms, value}]}` public results from the configured
  fallback and data-plane Canvas paths;
- exact graph-label extraction, including Canvas's explicit series selector;
- complete VictoriaMetrics export decoding with millisecond timestamps;
- all-or-error behavior for invalid JSON, a valid NDJSON prefix followed by an
  invalid row, and a connection closed before its declared content length;
- rejection of non-loopback client endpoints before a connection is opened;
- advisory rejection of a second Rust owner before it opens SQLite;
- explicit flush of two points, forced `SIGKILL` of the Rust OS pid, OTP child
  restart without a BEAM/UI supervisor crash, and the exact same two points
  after database reopen; and
- normal OTP shutdown after admitting an unflushed third point sends `SIGTERM`,
  exercises the server's graceful flush path, reaps the port child, leaves no
  orphan process, and recovers that third point on the next reopen.

Command and result:

```text
mix test test/timeless_ui/metrics_data_plane/client_test.exs \
  test/timeless_ui/metrics_data_plane/canvas_source_test.exs \
  test/metrics_data_plane_integration_test.exs

8 passed
```

The real-process test conditionally skips only when the sibling release server
or extension artifact has not been built. It ran against the Session 4 release
extension plus the Session 5 SIGTERM-aware server release artifact for this
result.

## Loopback boundary measurement

`timeless_ui/bench/metrics_data_plane_boundary.exs` seeded and flushed one
600-point series, warmed both paths, then ran five rounds alternating 500
complete export calls per path using either a pinned base URL or the
supervised-process readiness lookup. Both paths use the same Req transport,
full-body validation, NDJSON decode, and Canvas result normalization; the
difference across 2,500 samples per path isolates the incremental supervision
lookup.

| 600-point complete response | p50 | p95 | p99 |
|---|---:|---:|---:|
| client with pinned base URL | 695us | 933us | 1,014us |
| client via supervised process | 704us | 927us | 1,011us |

The measured p95 delta was -6us, which is noise: the readiness/supervisor
lookup adds no observable cost at this scale. Forced `SIGKILL` to a newly ready
listener took 20.08ms, and the first post-restart query returned the exact
flushed result.

This is a boundary microbenchmark, not a replacement for the Session 6 reader,
mixed-ingest, maintenance, HWM, and file-high-water matrix.

## Verdict

The Session 5 exit criterion is met. The boundary now works as an opt-in
deployment shape rather than only a standalone Rust benchmark:

- the Rust process is the sole telemetry database owner;
- a Rust crash is isolated from the BEAM and recovered by OTP;
- incomplete responses never become partial Canvas data;
- one real Canvas query retains its existing public result;
- both abnormal restart and normal shutdown have explicit lifecycle ownership;
- the Elixir product/control plane remains intact.

Packaging, production readiness probes, multi-node orchestration, signed
internal authorization, and broader Canvas/product adoption remain explicitly
outside this POC session.
