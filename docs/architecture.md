# Architecture

This document describes the current default architecture of TimelessMetrics.

The important versioned truth is:
- the default engine is the Rust engine
- the SQLite/libSQL block-store engine is available as an opt-in preview
- the legacy Elixir engine still exists, but it is no longer the primary design target

If you are reading older notes that describe ETS shard buffers, SegmentBuilder, ALP, or SQLite-backed raw storage as the hot path, those describe the legacy engine, not the default runtime on `main`.

## High-Level Design

TimelessMetrics is split into two layers:

1. Rust hot path
   - series resolution
   - labeled writes and batch writes
   - raw and aggregate reads
   - chunk persistence and restart recovery
   - label and metric listing

2. Elixir product layer
   - supervision and configuration
   - HTTP API
   - background ingest workers
   - alerts, annotations, metadata, scrape targets
   - PromQL execution and response shaping
   - charts, dashboard, forecasting, anomaly detection
   - retention, rollups, backup orchestration

The Rust layer is responsible for the time-series engine. Elixir is responsible for the surrounding application behavior.

## Supervision Tree

For a normal persisted store:

```elixir
children = [
  {TimelessMetrics, name: :metrics, data_dir: "/var/lib/metrics"},
  {TimelessMetrics.HTTP, store: :metrics, port: 8428}
]
```

Internally, the store supervisor starts different children depending on mode, but the current rust-default path is roughly:

```text
TimelessMetrics.Supervisor
├── TimelessMetrics.DB
├── TimelessMetrics.RustEngine
├── TimelessMetrics.IngestWorker x N   (non-memory mode)
├── TimelessMetrics.AlertEvaluator     (non-memory mode)
├── TimelessMetrics.SelfMonitor        (optional)
├── DynamicSupervisor                  (scraping enabled, non-memory mode)
└── TimelessMetrics.Scraper            (scraping enabled, non-memory mode)
```

`TimelessMetrics.HTTP` is a separate child you add alongside the store when you want HTTP ingest/query endpoints.

## Engine Selection

The default supervisor defaults to:

```elixir
engine: :rust
```

The legacy engine is still available through explicit configuration:

```elixir
{TimelessMetrics, name: :metrics, data_dir: "/tmp/metrics", engine: :legacy}
```

The single-database libSQL preview is also explicit:

```elixir
{TimelessMetrics, name: :metrics, data_dir: "/tmp/metrics", engine: :libsql}
```

Its supervision tree replaces `TimelessMetrics.RustEngine` with one
`TimelessMetrics.LibsqlEngine` writer and a pool of reader connections. Every
connection loads a small native SQLite extension that registers the
`timeless-libsql` virtual tables and query functions.

Current docs in this file describe the rust path unless stated otherwise.

## Write Path

### Programmatic API

Elixir writes call into `TimelessMetrics.RustEngine`, which forwards to the Rust NIF.

Flow:

```text
TimelessMetrics.write / write_batch
  -> TimelessMetrics.RustEngine
  -> Rust NIF
  -> resolve series
  -> append to in-memory partition buffers
  -> flush to chunk files when thresholds or maintenance triggers fire
```

Key properties:
- batch writes are the primary high-throughput path
- the engine maintains its own series registry
- chunk metadata is stored and rebuilt from files on restart
- writes remain queryable before and after flush

### HTTP Ingest

HTTP ingest is intentionally decoupled from parsing and storage:

```text
HTTP request
  -> handler enqueues raw body in ETS
  -> returns quickly
  -> IngestWorker drains queue
  -> parse body
  -> RustEngine/NIF write path
```

This is true for:
- VictoriaMetrics JSON-line ingest
- Prometheus text ingest
- Influx line protocol ingest

The queue is an Elixir concern. The actual time-series write path is still the Rust engine.

## Read Path

Raw and aggregate reads for the default engine go through the Rust layer.

Examples:
- `TimelessMetrics.query/4`
- `TimelessMetrics.query_multi/4`
- `TimelessMetrics.query_aggregate/4`
- `TimelessMetrics.query_aggregate_multi/4`

PromQL and HTTP compatibility endpoints are layered above that:

```text
HTTP / PromQL request
  -> parse HTTP params or PromQL
  -> call TimelessMetrics query functions
  -> RustEngine / NIF returns data
  -> Elixir formats HTTP / Prometheus response
```

So the Rust engine owns the data retrieval, but Elixir still owns:
- PromQL planning
- Prometheus response envelopes
- dashboard/chart formatting
- filtering and endpoint-specific shaping

## Storage Model

The rust engine persists data under a Rust-engine-specific directory inside the store `data_dir`.

Conceptually it keeps:
- a persisted series registry
- individual chunk files
- batched chunk files
- an in-memory index rebuilt on startup

Important behavior:
- chunk metadata is used to prune reads efficiently
- restart recovery rebuilds the in-memory index from disk
- chunk naming is designed to avoid restart-time overwrite collisions
- out-of-order points are normalized before chunk metadata is written

### libSQL preview storage model

The libSQL engine uses the same `data_dir/metrics.db` that already holds admin
state. `timeless-libsql` shadow tables in that database hold the series
registry, compressed raw blocks, rollups, retention metadata, and engine
statistics. Inserts go through the `timeless_metrics` virtual table; wide
range queries use the one-row, multi-series `timeless_raw_frame` table-valued
function and decode its versioned `TRF1` columns directly into final BEAM
series maps (selective exact reads retain the per-series batch path); scalar
aggregates use the chunk-aware `timeless_aggregate` kernel;
latest-point reads use the newest-first `timeless_latest` kernel; rollup
queries use one prepared `timeless_rollup_batches` call and decode the complete
stored bucket record once per series. Complete `from`-aligned
`avg`/`sum`/`min`/`max`/`count` buckets map to `timeless_window_batches`, which
returns one versioned timestamp/value blob per series. Partial terminal buckets
and counter/ordering operations remain raw because their semantics differ from
the native half-open window. The scalar/latest/window/rollup adapters select
series ids plus numeric data from SQLite and resolve immutable labels through
the ETS catalog cache, avoiding repeated JSON transport and decoding. The raw
frame is bounded to 10x the returned external-term size in the 12K-series
benchmark; the measured peak increment was 120,596,408 bytes for a 12,959,022
byte result (9.306x).

The boundary is intentionally narrow:

```text
TimelessMetrics public API
  -> Elixir matcher/query semantics
  -> one SQLite writer or pooled SQLite reader
  -> timeless-libsql virtual table
  -> shadow tables and compressed blocks in metrics.db
```

This design makes a SQLite snapshot of `metrics.db` a complete backup. The
Rust-to-libSQL converter stages and verifies a replacement database before an
explicit activation; there is no implicit dual-read or dual-write mode.

## Memory-Only Mode

Memory-only mode disables durable raw-data persistence:

```elixir
{TimelessMetrics, name: :metrics, mode: :memory}
```

In memory-only mode:
- the rust engine still serves as the hot path
- scraping and alert evaluator are skipped
- raw series data is not persisted for recovery

This mode is useful for:
- tests
- local experiments
- ephemeral services
- constrained deployments

## Metadata and Admin Data

The Rust engine handles time-series storage, but the DB process still matters.

Elixir-side admin data is still managed in SQLite-backed tables through `TimelessMetrics.DB`, including:
- metric metadata
- annotations
- alert rules and state
- scrape targets and scrape health
- rollup/admin metadata used by the higher-level product surface

That means the system is not “Rust only.” It is a Rust-default time-series engine with an Elixir application layer around it.

## HTTP Surface

`TimelessMetrics.HTTP` exposes three groups of endpoints:

1. Native ingest/query
   - `/api/v1/import`
   - `/api/v1/import/prometheus`
   - `/write`
   - `/api/v1/query`
   - `/api/v1/query_range`
   - `/api/v1/export`

2. Prometheus-compatible endpoints
   - `/prometheus/api/v1/query`
   - `/prometheus/api/v1/query_range`
   - `/prometheus/api/v1/labels`
   - `/prometheus/api/v1/label/:name/values`
   - `/prometheus/api/v1/series`

3. Product/ops endpoints
   - `/health`
   - `/health/detailed`
   - `/chart`
   - annotations, alerts, metadata, backup, dashboard, forecasting, anomalies

## Benchmarks

The benchmark set was cleaned up to reflect the current architecture. See:
- [../bench/README.md](../bench/README.md)

The maintained benchmarks are:
- embedded API throughput
- HTTP concurrency
- realistic HTTP workload ramp
- TSBS harness
- VictoriaMetrics comparison

## Legacy Notes

If you need the legacy engine, keep these distinctions in mind:
- old docs describing ETS shard buffers and SegmentBuilder are about the legacy path
- old compression references to ALP as the primary active engine are legacy descriptions
- old benchmark scripts that depended on actor-era internals were intentionally removed

The codebase still contains compatibility paths, but the primary architecture is now the rust-default engine described above.
