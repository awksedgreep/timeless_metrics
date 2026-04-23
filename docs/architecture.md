# Architecture

This document describes the current default architecture of TimelessMetrics.

The important versioned truth is:
- the default engine is the Rust engine
- the legacy Elixir engine still exists, but it is no longer the primary design target

If you are reading older notes that describe ETS shard buffers, SegmentBuilder, Gorilla, ALP, or SQLite-backed raw storage as the hot path, those describe the legacy engine, not the default runtime on `main`.

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
- old compression references to Gorilla or ALP as the primary active engine are legacy descriptions
- old benchmark scripts that depended on actor-era internals were intentionally removed

The codebase still contains compatibility paths, but the primary architecture is now the rust-default engine described above.
