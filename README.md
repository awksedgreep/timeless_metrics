<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/logo-light.svg">
    <img src="docs/logo-light.svg" width="300" alt="Timeless">
  </picture>
</p>

<h3 align="center">Embedded Time Series Database for Elixir</h3>

<p align="center">
  <a href="https://hex.pm/packages/timeless_metrics"><img src="https://img.shields.io/hexpm/v/timeless_metrics.svg" alt="Hex.pm"></a>
  <a href="https://hexdocs.pm/timeless_metrics"><img src="https://img.shields.io/badge/docs-hexdocs-blue.svg" alt="Docs"></a>
  <a href="LICENSE"><img src="https://img.shields.io/hexpm/l/timeless_metrics.svg" alt="License"></a>
</p>

---

> "I found it ironic that the first thing you do to time series data is squash the timestamp. That's how the name Timeless was born." --Mark Cotner

TimelessMetrics is an embedded time-series database for Elixir with a Rust-native hot path, built-in HTTP ingest/query APIs, retention, rollups, alerting, scraping, charts, and Prometheus/VictoriaMetrics compatibility.

It runs:
- as a library inside your Elixir application
- as a local service with the included HTTP API
- in memory-only mode for tests and constrained environments

## Current Architecture

The default engine is the Rust engine.

Hot-path responsibilities live in the Rust NIF:
- labeled writes and batched writes
- raw and aggregate range queries
- series index and label filtering
- chunk persistence and recovery

Elixir still owns the surrounding product surface:
- HTTP API
- background ingest workers
- alerts, annotations, metadata, and scrape target management
- rollups, retention orchestration, charts, dashboard, and PromQL handling

If you need the detailed design, start with [docs/architecture.md](docs/architecture.md).

## Documentation

- [Getting Started](docs/getting_started.md)
- [Configuration Reference](docs/configuration.md)
- [Architecture](docs/architecture.md)
- [API Reference](docs/API.md)
- [Alerting](docs/alerting.md)
- [Scraping](docs/scraping.md)
- [Annotations](docs/annotations.md)
- [Forecasting & Anomaly Detection](docs/forecasting.md)
- [Charts & Embedding](docs/charts.md)
- [Grafana Integration](docs/grafana.md)
- [Operations](docs/operations.md)
- [Capacity Planning](docs/capacity_planning.md)
- [Scaling](docs/scaling_options.md)
- [Benchmarks](bench/README.md)

## Highlights

- Rust-native storage engine enabled by default
- Embedded Elixir API plus optional HTTP API
- Prometheus text ingest, VictoriaMetrics JSON-line ingest, and Influx line protocol ingest
- Prometheus-compatible query endpoints for Grafana
- Fast batched writes and compact on-disk chunks
- Built-in dashboard, SVG charts, annotations, forecasting, anomaly detection, and alerts
- Scraping subsystem for pulling Prometheus targets into the local store
- Memory-only mode for ephemeral deployments and tests

## Quick Start

Add to `mix.exs`:

```elixir
{:timeless_metrics, "~> 6.0"}
```

Add to your supervision tree:

```elixir
children = [
  {TimelessMetrics, name: :metrics, data_dir: "/var/lib/metrics"},
  {TimelessMetrics.HTTP, store: :metrics, port: 8428}
]
```

Write and query:

```elixir
TimelessMetrics.write(:metrics, "cpu_usage", %{"host" => "web-1"}, 73.2)

{:ok, points} =
  TimelessMetrics.query(:metrics, "cpu_usage", %{"host" => "web-1"},
    from: System.os_time(:second) - 3600,
    to: System.os_time(:second)
  )
```

Memory-only mode:

```elixir
children = [
  {TimelessMetrics, name: :metrics, mode: :memory},
  {TimelessMetrics.HTTP, store: :metrics, port: 8428}
]
```

## Elixir API

Writes:

```elixir
TimelessMetrics.write(:metrics, "cpu_usage", %{"host" => "web-1"}, 73.2)

TimelessMetrics.write_batch(:metrics, [
  {"cpu_usage", %{"host" => "web-1"}, 73.2},
  {"mem_usage", %{"host" => "web-1"}, 4096.0}
])
```

Queries:

```elixir
{:ok, points} =
  TimelessMetrics.query(:metrics, "cpu_usage", %{"host" => "web-1"},
    from: System.os_time(:second) - 3600
  )

{:ok, series} =
  TimelessMetrics.query_multi(:metrics, "cpu_usage", %{"host" => "web-1"},
    from: System.os_time(:second) - 3600
  )

{:ok, buckets} =
  TimelessMetrics.query_aggregate(:metrics, "cpu_usage", %{"host" => "web-1"},
    from: System.os_time(:second) - 3600,
    bucket: {60, :seconds},
    aggregate: :avg
  )
```

Discovery and operations:

```elixir
TimelessMetrics.list_metrics(:metrics)
TimelessMetrics.list_series(:metrics, "cpu_usage")
TimelessMetrics.label_values(:metrics, "cpu_usage", "host")
TimelessMetrics.info(:metrics)
TimelessMetrics.flush(:metrics)
TimelessMetrics.backup(:metrics, "/tmp/metrics-backup")
```

## HTTP API

Core endpoints:

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/import` | VictoriaMetrics JSON-line ingest |
| `POST` | `/api/v1/import/prometheus` | Prometheus text ingest |
| `POST` | `/write` | Influx line protocol ingest |
| `GET` | `/api/v1/query` | Latest-value query |
| `GET` | `/api/v1/query_range` | Native range query |
| `GET` | `/api/v1/export` | Multi-series export |
| `GET` | `/prometheus/api/v1/query` | Prometheus instant query |
| `GET` | `/prometheus/api/v1/query_range` | Prometheus range query |
| `GET` | `/prometheus/api/v1/labels` | Prometheus label names |
| `GET` | `/prometheus/api/v1/series` | Prometheus series listing |
| `GET` | `/chart` | SVG chart |
| `GET` | `/health` | Lightweight health/status |
| `GET` | `/health/detailed` | More expensive store diagnostics |

Example ingest:

```bash
curl -X POST http://localhost:8428/api/v1/import/prometheus \
  -H "Content-Type: text/plain" \
  --data-binary '
cpu_usage{host="web-1"} 73.2
cpu_usage{host="web-2"} 61.8
'
```

Example range query:

```bash
curl 'http://localhost:8428/api/v1/query_range?metric=cpu_usage&host=web-1&from=1700000000&to=1700003600&step=60'
```

Example Prometheus-compatible query:

```bash
curl 'http://localhost:8428/prometheus/api/v1/query_range?query=cpu_usage{host="web-1"}&start=1700000000&end=1700003600&step=60'
```

## Benchmarks

The maintained benchmark set lives under [bench/](bench/README.md):
- embedded API throughput: [bench/write_bench.exs](bench/write_bench.exs)
- HTTP concurrency: [bench/http_concurrency.exs](bench/http_concurrency.exs)
- realistic workload ramp: [bench/realistic_workload.exs](bench/realistic_workload.exs)
- TSBS harness: [bench/tsbs_bench.exs](bench/tsbs_bench.exs)
- VictoriaMetrics comparison: [bench/vs_victoriametrics.exs](bench/vs_victoriametrics.exs)

## Notes

- The legacy Elixir engine still exists behind explicit `engine:` selection for compatibility and migration work, but the default path is Rust.
- The rust build may emit the upstream `rustler::resource!` `non_local_definitions` warning. That warning is currently expected.
