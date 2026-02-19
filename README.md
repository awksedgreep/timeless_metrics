# Timeless

Embedded time series database for Elixir. Combines [Gorilla compression](https://github.com/awksedgreep/gorilla_stream) with SQLite for fast, compact metric storage with automatic rollups and configurable retention.

Run it as a library inside your Elixir app or as a standalone container.

## Features

- **High throughput** — 4M+ points/sec ingest, 9.5M+ points/sec concurrent pre-resolved writes
- **Compact storage** — Gorilla + zstd compression, ~0.67 bytes/point for real-world data
- **Sharded writes** — parallel buffer/builder shards across CPU cores
- **Automatic rollups** — configurable tiers (hourly, daily, monthly) with retention policies
- **VictoriaMetrics compatible** — JSON line import, works with Vector, Grafana, and existing VM tooling
- **Prometheus compatible** — text exposition import and PromQL-compatible query endpoint for Grafana
- **SVG charts** — pure Elixir chart rendering, embeddable via `<img>` tags with light/dark/auto themes
- **Built-in dashboard** — zero-dependency HTML overview with auto-refresh
- **Annotations** — event markers (deploys, incidents) that overlay on charts
- **Forecasting** — polynomial trend + Fourier seasonal regression, auto-detects daily/weekly/yearly periods
- **Anomaly detection** — z-score analysis on model residuals with configurable sensitivity
- **Capacity planning** — forecast from daily/weekly data to predict growth months or years ahead
- **Alerts** — threshold-based rules with webhook notifications (ntfy.sh, Slack, etc.)
- **Metric metadata** — type, unit, and description registration
- **Zero external dependencies** — SQLite + pure Elixir, no Nx/Scholar/ML libraries required

## Quick Start

### As a library

Add to your `mix.exs`:

```elixir
{:timeless, github: "awksedgreep/timeless"}
```

Add to your supervision tree:

```elixir
children = [
  {Timeless, name: :metrics, data_dir: "/var/lib/metrics"},
  {Timeless.HTTP, store: :metrics, port: 8428}
]
```

Write and query:

```elixir
Timeless.write(:metrics, "cpu_usage", %{"host" => "web-1"}, 73.2)

{:ok, points} = Timeless.query(:metrics, "cpu_usage", %{"host" => "web-1"},
  from: System.os_time(:second) - 3600)
```

### As a container

```bash
podman build -f Containerfile -t timeless:latest .
podman run -d -p 8428:8428 -v timeless_data:/data:Z localhost/timeless:latest
```

Ingest data:

```bash
curl -X POST http://localhost:8428/api/v1/import -d '
{"metric":{"__name__":"cpu_usage","host":"web-1"},"values":[73.2],"timestamps":[1700000000]}'
```

Query:

```bash
curl 'http://localhost:8428/api/v1/query_range?metric=cpu_usage&from=-1h&step=60'
```

Embed a chart with forecast and anomaly overlays:

```html
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-6h&forecast=1h&anomalies=medium&theme=auto" />
```

Forecast future values:

```bash
curl 'http://localhost:8428/api/v1/forecast?metric=cpu_usage&from=-24h&step=300&horizon=6h'
```

Detect anomalies:

```bash
curl 'http://localhost:8428/api/v1/anomalies?metric=cpu_usage&from=-24h&step=300&sensitivity=medium'
```

View the dashboard at `http://localhost:8428/`.

## HTTP Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/import` | VictoriaMetrics JSON line ingest |
| `POST` | `/api/v1/import/prometheus` | Prometheus text format ingest |
| `GET` | `/api/v1/export` | Export raw points (VM JSON line format) |
| `GET` | `/api/v1/query` | Latest value for matching series |
| `GET` | `/api/v1/query_range` | Range query with bucketed aggregation |
| `GET` | `/prometheus/api/v1/query_range` | Grafana-compatible Prometheus endpoint |
| `GET` | `/api/v1/label/__name__/values` | List all metric names |
| `GET` | `/api/v1/label/:name/values` | List values for a label key |
| `GET` | `/api/v1/series` | List series for a metric |
| `POST` | `/api/v1/metadata` | Register metric metadata |
| `GET` | `/api/v1/metadata` | Get metric metadata |
| `POST` | `/api/v1/annotations` | Create an annotation |
| `GET` | `/api/v1/annotations` | Query annotations |
| `DELETE` | `/api/v1/annotations/:id` | Delete an annotation |
| `POST` | `/api/v1/alerts` | Create an alert rule |
| `GET` | `/api/v1/alerts` | List alert rules |
| `DELETE` | `/api/v1/alerts/:id` | Delete an alert rule |
| `GET` | `/api/v1/forecast` | Forecast future values |
| `GET` | `/api/v1/anomalies` | Detect anomalies |
| `GET` | `/chart` | SVG chart (supports `&forecast=` and `&anomalies=` overlays) |
| `GET` | `/health` | Health check with stats |
| `GET` | `/` | HTML dashboard |

See [docs/API.md](docs/API.md) for full request/response documentation with examples.

## Forecasting & Anomaly Detection

Timeless includes a built-in forecast engine and anomaly detector — no external ML libraries needed. The pure Elixir normal equation solver runs in ~3ms for a year of daily data.

Seasonal periods are auto-detected from the data's sampling interval:

| Sampling | Periods | Use Case |
|---|---|---|
| Sub-hourly | Daily + half-daily | Operational monitoring |
| Hourly | Daily + weekly | Trend analysis |
| Daily+ | Weekly + yearly | Capacity planning |

### Elixir API

```elixir
# Forecast 6 hours ahead from 24h of 5-minute data
{:ok, results} = Timeless.forecast(:metrics, "cpu_usage", %{"host" => "web-1"},
  from: now - 86_400, horizon: 21_600, bucket: {300, :seconds})

# Detect anomalies
{:ok, results} = Timeless.detect_anomalies(:metrics, "cpu_usage", %{"host" => "web-1"},
  from: now - 86_400, bucket: {300, :seconds}, sensitivity: :medium)

# Capacity planning: 1 year of daily data → 1 year forecast
{:ok, results} = Timeless.forecast(:metrics, "bandwidth_peak", %{},
  from: now - 365 * 86_400, horizon: 365 * 86_400, bucket: {1, :days}, aggregate: :max)
```

### Chart overlays

```html
<!-- Purple dashed forecast line + red anomaly dots -->
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-24h&forecast=6h&anomalies=medium" />
```

![Forecast + anomaly chart](docs/examples/chart_full_analysis.svg)

See [docs/capacity_planning.md](docs/capacity_planning.md) for detailed capacity planning guide with ISP examples.

See [docs/alerting.md](docs/alerting.md) for alert rules, webhook payloads, and integration with ntfy.sh/Slack.

## Container Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `TIMELESS_DATA_DIR` | `/data` | SQLite storage directory |
| `TIMELESS_PORT` | `8428` | HTTP listen port |
| `TIMELESS_SHARDS` | CPU count | Write buffer shard count |
| `TIMELESS_SEGMENT_DURATION` | `14400` | Raw segment duration (seconds) |
| `TIMELESS_BEARER_TOKEN` | *(none)* | Bearer token for API auth (unset = no auth) |

## Authentication

Set `TIMELESS_BEARER_TOKEN` to require authentication on all endpoints (except `/health`):

```bash
# Via env var
TIMELESS_BEARER_TOKEN=my-secret-token podman run -d -p 8428:8428 ...

# API access
curl -H "Authorization: Bearer my-secret-token" http://localhost:8428/api/v1/query_range?...

# Browser access (dashboard, charts) via query param
http://localhost:8428/chart?metric=cpu_usage&from=-6h&token=my-secret-token
```

When unset, all endpoints are open (backwards compatible, for trusted networks).

## Podman Quadlet

Copy `timeless.container` to `~/.config/containers/systemd/`:

```bash
cp timeless.container ~/.config/containers/systemd/
systemctl --user daemon-reload
systemctl --user start timeless
```

## Architecture

```
Writes ──> Buffer Shards (ETS) ──> Segment Builders ──> SQLite Shard DBs
                                                              │
                                              Rollup Engine ──┤── hourly tier
                                                              ├── daily tier
                                                              └── monthly tier

Main DB: series registry, metadata, annotations, alerts
Shard DBs: raw_segments, tier tables, watermarks
```

- **Buffer shards** — lock-free ETS tables, one per CPU core, flushed every 5s or 10K points
- **Segment builders** — Gorilla + zstd compression, one per shard for parallel writes
- **Rollup engine** — parallel per-shard aggregation into configurable tiers
- **Retention enforcer** — periodic cleanup of expired raw data and tier rows
- **SQLite** — WAL mode, mmap, WITHOUT ROWID for clustered B-trees, 16KB pages

## Custom Rollup Schema

```elixir
defmodule MyApp.MetricsSchema do
  use Timeless.Schema

  raw_retention {7, :days}

  tier :hourly,
    resolution: :hour,
    aggregates: [:avg, :min, :max, :count, :sum, :last],
    retention: {30, :days}

  tier :daily,
    resolution: :day,
    aggregates: [:avg, :min, :max, :count, :sum, :last],
    retention: {365, :days}

  tier :monthly,
    resolution: {30, :days},
    aggregates: [:avg, :min, :max, :count, :sum, :last],
    retention: :forever
end
```

```elixir
{Timeless, name: :metrics, data_dir: "/data", schema: MyApp.MetricsSchema}
```

## License

MIT
