# Deferred Rust telemetry data-plane POC

Status: captured for a fresh branch after the libSQL storage migration and its
query-performance gates are complete. This proposal is deliberately out of
scope for `feat/libsql-storage-engine`; the work on this branch remains useful
to it unchanged.

## Motivation

Timeless currently implements three separate Rocket HTTP surfaces in Elixir:

- `TimelessMetrics.HTTP` for Prometheus/VictoriaMetrics-compatible ingest,
  query, discovery, and metrics product routes
- `TimelessLogs.HTTP` for VictoriaLogs-compatible ingest and LogsQL queries
- `TimelessTraces.HTTP` for OTLP ingest and Jaeger-compatible queries

Rocket's routing and connection management are Elixir, but its HTTP parser is
a C NIF. More importantly, query work is split across Rust storage, SQLite,
NIF/Exqlite boundaries, and Elixir evaluation and response shaping. Moving the
socket loop into another NIF would remove the handwritten C parser but retain
the same fault-domain, lifecycle, cancellation, and scheduler coupling.

The proposed POC instead tests a standalone Rust telemetry data-plane process.
The goal is not to rewrite the UI or product workflow in Rust.

## Proposed boundary

```text
Prometheus / OTel / Grafana / Jaeger / agents
                         |
                         v
                   timelessd (Rust)
       HTTP, protocol parsing, query execution, response encoding,
       limits, backpressure, retention, compaction, flush, backup
                         |
                         v
          timeless-query + timeless-libsql extension
                         |
                         v
               Rust-owned telemetry database

Timeless UI / Canvas / Stack (Elixir)
       sessions, dashboards, LiveView, product orchestration
                         |
             loopback HTTP or Unix socket
                         v
                    timelessd
```

### Rust data-plane ownership

- Prometheus and VictoriaMetrics ingest, query, labels, series, metadata, and
  export compatibility endpoints
- VictoriaLogs ingest, LogsQL parsing/evaluation, field discovery, search, and
  streaming/tailing
- OTLP JSON/protobuf ingest, trace search/assembly, and Jaeger responses
- PromQL parsing, planning, and evaluation
- protocol decoding plus final JSON, NDJSON, and protobuf serialization
- telemetry database connections, transactions, limits, cancellation, and
  backpressure
- flush, compaction, retention, backup, and telemetry health/statistics

### Elixir product/control-plane ownership

- Phoenix, LiveView, Canvas, dashboards, and cross-signal presentation
- users, sessions, product authorization, and human configuration
- alert scheduling/state and notification delivery; Rust evaluates the query
- scrape scheduling and target management; Rust accepts the resulting samples
- annotations and other product metadata
- forecasting/anomaly product policy, with optional Rust compute kernels later
- deployment orchestration and supervision of the Rust child process

The current metrics HTTP module mixes these layers. Compatibility data routes
should move to Rust; alerts, annotations, scrape-target CRUD, HTML, charts, and
other product routes do not need to move with them.

## Boundary rules

1. Use a process/socket boundary for the server, not an HTTP accept loop inside
   a NIF. A crash in the data plane must not take down the BEAM UI.
2. Rust exclusively owns telemetry database connections during normal stack
   operation. Do not create competing Elixir and Rust database owners merely
   because SQLite permits concurrent connections.
3. Cross the boundary once per complete operation. Send a query and return its
   final vector, matrix, trace, log page, or serialized response; never shuttle
   blocks or individual points through repeated NIF calls.
4. Keep public protocol compatibility and existing ports during migration. One
   process may listen on the metrics, logs, and traces legacy ports before a
   unified endpoint is introduced.
5. Keep one implementation of query semantics. The server, extension, and any
   optional embedded interface must reuse the same Rust query crates.
6. Keep a raw storage fallback for every accelerated query operation. PromQL
   policy must not leak into low-level storage primitives simply to improve a
   benchmark.
7. If product authentication remains in Elixir, Rust must validate a signed
   internal tenant claim or API credential and independently enforce limits.

All three telemetry signals may remain in one Rust-owned database. Elixir
product state may use a separate small database. If a single artifact for both
telemetry and product state becomes a requirement, product tables should be
accessed through a Rust control API rather than opened by two owners.

## Candidate Rust layout

Build on the existing `timeless-codec`, `timeless-core`, and `timeless-ext`
crates:

- `timeless-query`: storage-independent PromQL, LogsQL, and trace query
  planning/evaluation over explicit storage traits
- `timeless-protocol`: Prometheus, VictoriaMetrics, VictoriaLogs, OTLP, and
  Jaeger request/response models and codecs
- `timeless-api`: route handlers, authentication hooks, limits, cancellation,
  streaming, and response construction
- `timelessd`: standalone server binary which loads or embeds the existing
  extension and owns the database connections
- `TimelessStack.DataClient`: a thin Elixir client used by Canvas and Stack

The loadable extension remains a first-class product for direct SQLite/libSQL
users. Reusable query functionality should live above the extension in Rust;
add SQL table-valued surfaces only where SQLite callback and re-entrancy rules
remain clean. This gives users outside Timeless a loadable extension, reusable
Rust query crates, and a standalone server without requiring the BEAM.

## POC scope and success criteria

Create the POC on a fresh branch from the completed storage/query migration,
not on `feat/libsql-storage-engine`.

The first vertical slice should use metrics only:

1. Start `timelessd` as a separately supervised OS child.
2. Load the same `timeless-libsql` extension and own one metrics database.
3. Implement health, Prometheus text ingest, exact/raw range query, labels, and
   one aggregate query through the existing public storage interfaces.
4. Return final wire-format responses from Rust without materializing points in
   the BEAM.
5. Add an Elixir client adapter and switch one Canvas query path without
   changing its public result.
6. Run byte/semantic differential tests against the current Elixir HTTP API and
   compare throughput, latency, peak memory, cancellation, and crash isolation.

POC success means:

- no C or Rust HTTP NIF is present in the normal request path
- the Rust process can crash and restart without crashing the BEAM
- existing clients see compatible responses
- the database has one clear runtime owner
- one wide query avoids the Rust -> SQLite -> Exqlite -> Elixir expansion path
- the architecture can add logs and traces without introducing signal-specific
  process boundaries or duplicate storage/query semantics

## Later migration sequence

If the metrics POC succeeds:

1. Freeze all current routes with black-box compatibility fixtures.
2. Move ingest for all three signals.
3. Move logs and traces reads, then metrics discovery and mechanical queries.
4. Port PromQL last, using the established VictoriaMetrics differential corpus.
5. Replace direct Stack calls with `TimelessStack.DataClient`.
6. Move storage operations and remove Rocket from the three telemetry apps.
7. Decide separately whether any remaining product routes belong in Rust.

A useful metrics vertical slice is expected to take several weeks. Full
three-signal API, PromQL, packaging, and production hardening is a multi-month
project; compatibility and operational correctness are the larger risks than
route translation.
