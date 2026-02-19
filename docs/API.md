# TimelessMetrics API Reference

TimelessMetrics is an embedded time series database for Elixir. It can run as a library
inside your application or as a standalone containerized service. This document
covers the Elixir API, the HTTP interface, and the SVG charting endpoint.

---

## Table of Contents

- [Elixir API](#elixir-api)
  - [Starting a Store](#starting-a-store)
  - [Writing Data](#writing-data)
  - [Pre-Resolved Writes (High-Throughput)](#pre-resolved-writes-high-throughput)
  - [Querying Data](#querying-data)
  - [Aggregation Queries](#aggregation-queries)
  - [Tier Queries](#tier-queries)
  - [Series Discovery](#series-discovery)
  - [Metric Metadata](#metric-metadata)
  - [Annotations](#annotations)
  - [Alerts](#alerts)
  - [Operational](#operational)
- [HTTP API](#http-api)
  - [Authentication](#authentication)
  - [Ingest Endpoints](#ingest-endpoints)
  - [Query Endpoints](#query-endpoints)
  - [Prometheus-Compatible Endpoints](#prometheus-compatible-endpoints)
  - [Series Discovery Endpoints](#series-discovery-endpoints)
  - [Metadata Endpoints](#metadata-endpoints)
  - [Annotation Endpoints](#annotation-endpoints)
  - [Alert Endpoints](#alert-endpoints)
  - [Charts and Dashboard](#charts-and-dashboard)
  - [Health Check](#health-check)
- [SVG Charts](#svg-charts)
  - [Embedding Charts](#embedding-charts)
  - [Chart Parameters](#chart-parameters)
  - [Themes](#themes)
  - [Programmatic Usage](#programmatic-usage)
- [Configuration](#configuration)
  - [Store Options](#store-options)
  - [Schema and Rollup Tiers](#schema-and-rollup-tiers)
  - [Environment Variables (Container)](#environment-variables-container)

---

## Elixir API

### Starting a Store

Add TimelessMetrics to your supervision tree:

```elixir
children = [
  {TimelessMetrics, name: :metrics, data_dir: "/var/lib/metrics"}
]
```

All subsequent API calls reference the store by its name (`:metrics` above).

#### Store Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:name` | atom | **required** | Store name used in all API calls |
| `:data_dir` | string | **required** | Directory for segment files and metadata database |
| `:buffer_shards` | integer | `schedulers / 2` (min 2) | Number of write buffer shards |
| `:flush_interval` | integer | `5000` | Buffer flush interval in ms |
| `:flush_threshold` | integer | `10_000` | Points per shard before forced flush |
| `:segment_duration` | integer | `3600` | Raw segment duration in seconds |
| `:compression` | atom | `:zstd` | Compression algorithm |
| `:schema` | module/struct | `TimelessMetrics.Schema.default()` | Rollup tier configuration |

### Writing Data

#### Single point

```elixir
TimelessMetrics.write(:metrics, "cpu_usage", %{"host" => "web-1"}, 73.2)

# With explicit timestamp (unix seconds)
TimelessMetrics.write(:metrics, "cpu_usage", %{"host" => "web-1"}, 73.2,
  timestamp: 1_700_000_000)
```

#### Batch write

Each entry is `{metric, labels, value}` or `{metric, labels, value, timestamp}`:

```elixir
TimelessMetrics.write_batch(:metrics, [
  {"cpu_usage", %{"host" => "web-1"}, 73.2},
  {"cpu_usage", %{"host" => "web-2"}, 81.0},
  {"mem_usage", %{"host" => "web-1"}, 4_200_000, 1_700_000_000}
])
```

### Pre-Resolved Writes (High-Throughput)

For hot paths (hundreds of thousands of writes per second), resolve series IDs
once at startup and bypass the registry on every write:

```elixir
# Resolve once (e.g., at poller init)
sid = TimelessMetrics.resolve_series(:metrics, "cpu_usage", %{"host" => "web-1"})

# Write on the hot path — zero registry overhead
TimelessMetrics.write_resolved(:metrics, sid, 73.2)
TimelessMetrics.write_resolved(:metrics, sid, 74.1, timestamp: 1_700_000_060)
```

#### Pre-resolved batch

Each entry is `{series_id, value}` or `{series_id, value, timestamp}`:

```elixir
TimelessMetrics.write_batch_resolved(:metrics, [
  {sid_1, 73.2},
  {sid_2, 81.0, 1_700_000_000}
])
```

### Querying Data

#### Raw points (single series, exact label match)

```elixir
{:ok, points} = TimelessMetrics.query(:metrics, "cpu_usage", %{"host" => "web-1"},
  from: System.os_time(:second) - 3600,
  to: System.os_time(:second))

# points = [{1700000000, 73.2}, {1700000060, 74.1}, ...]
```

#### Raw points (multi-series, label filter)

An empty label filter matches all series for the metric:

```elixir
{:ok, results} = TimelessMetrics.query_multi(:metrics, "cpu_usage", %{})

# results = [
#   %{labels: %{"host" => "web-1"}, points: [{ts, val}, ...]},
#   %{labels: %{"host" => "web-2"}, points: [{ts, val}, ...]},
# ]
```

Filter by partial labels:

```elixir
{:ok, results} = TimelessMetrics.query_multi(:metrics, "cpu_usage",
  %{"region" => "us-east"},
  from: now - 7200, to: now)
```

#### Latest value

```elixir
{:ok, {timestamp, value}} = TimelessMetrics.latest(:metrics, "cpu_usage", %{"host" => "web-1"})
{:ok, nil} = TimelessMetrics.latest(:metrics, "nonexistent", %{})
```

### Aggregation Queries

Bucket data into time intervals with an aggregate function:

```elixir
{:ok, buckets} = TimelessMetrics.query_aggregate(:metrics, "cpu_usage",
  %{"host" => "web-1"},
  from: now - 86400,
  to: now,
  bucket: {300, :seconds},
  aggregate: :avg)

# buckets = [{1700000000, 73.5}, {1700000300, 74.2}, ...]
```

#### Multi-series aggregation

```elixir
{:ok, results} = TimelessMetrics.query_aggregate_multi(:metrics, "cpu_usage", %{},
  from: now - 86400,
  to: now,
  bucket: :hour,
  aggregate: :max)

# results = [
#   %{labels: %{"host" => "web-1"}, data: [{bucket_ts, max_val}, ...]},
#   %{labels: %{"host" => "web-2"}, data: [{bucket_ts, max_val}, ...]},
# ]
```

#### Bucket sizes

- `:minute`, `:hour`, `:day` — named intervals
- `{n, :seconds}` — arbitrary interval (e.g., `{300, :seconds}` for 5 min)

#### Aggregate functions

| Function | Description |
|----------|-------------|
| `:avg` | Mean value in bucket |
| `:min` | Minimum value |
| `:max` | Maximum value |
| `:sum` | Sum of all values |
| `:count` | Number of points |
| `:first` | First value in bucket |
| `:last` | Last value in bucket |
| `:rate` | Per-second rate of change |

### Tier Queries

Read pre-computed rollup data directly from a tier:

```elixir
{:ok, rows} = TimelessMetrics.query_tier(:metrics, :hourly, "cpu_usage",
  %{"host" => "web-1"},
  from: now - 86400, to: now)

# rows = [%{bucket: ts, avg: v, min: v, max: v, count: n, sum: v, last: v}, ...]
```

### Series Discovery

```elixir
# List all metric names
{:ok, names} = TimelessMetrics.list_metrics(:metrics)
# ["cpu_usage", "mem_usage", "disk_io"]

# List all series for a metric
{:ok, series} = TimelessMetrics.list_series(:metrics, "cpu_usage")
# [%{labels: %{"host" => "web-1"}}, %{labels: %{"host" => "web-2"}}]

# List distinct values for a label key
{:ok, hosts} = TimelessMetrics.label_values(:metrics, "cpu_usage", "host")
# ["web-1", "web-2", "web-3"]
```

### Metric Metadata

```elixir
# Register metadata
TimelessMetrics.register_metric(:metrics, "cpu_usage", :gauge,
  unit: "%",
  description: "CPU utilization percentage")

# Get metadata
{:ok, meta} = TimelessMetrics.get_metadata(:metrics, "cpu_usage")
# %{type: :gauge, unit: "%", description: "CPU utilization percentage"}
```

Metric types: `:gauge`, `:counter`, `:histogram`

### Annotations

Annotations are event markers that overlay on charts (deploys, incidents, etc.):

```elixir
# Create
{:ok, id} = TimelessMetrics.annotate(:metrics, System.os_time(:second), "Deploy v2.1",
  description: "Rolled out new caching layer",
  tags: ["deploy", "prod"])

# Query time range
{:ok, annotations} = TimelessMetrics.annotations(:metrics, from, to, tags: ["deploy"])
# [%{id: 1, timestamp: ts, title: "Deploy v2.1", description: "...", tags: ["deploy", "prod"]}]

# Delete
TimelessMetrics.delete_annotation(:metrics, id)
```

### Alerts

```elixir
# Create alert rule
{:ok, rule_id} = TimelessMetrics.create_alert(:metrics,
  name: "High CPU",
  metric: "cpu_usage",
  condition: :above,
  threshold: 90.0,
  labels: %{"host" => "web-1"},
  duration: 300,
  aggregate: :avg,
  webhook_url: "http://hooks.example.com/alert")

# List all rules with current state
{:ok, rules} = TimelessMetrics.list_alerts(:metrics)

# Evaluate all rules (also runs automatically on a timer)
TimelessMetrics.evaluate_alerts(:metrics)

# Delete a rule
TimelessMetrics.delete_alert(:metrics, rule_id)
```

### Operational

```elixir
# Flush all buffered data to disk
TimelessMetrics.flush(:metrics)

# Get store statistics
info = TimelessMetrics.info(:metrics)
# %{series_count: 1000, total_points: 5_000_000, bytes_per_point: 0.78, ...}

# Force rollup
TimelessMetrics.rollup(:metrics)         # all tiers
TimelessMetrics.rollup(:metrics, :hourly) # specific tier

# Force late-arrival catch-up scan
TimelessMetrics.catch_up(:metrics)

# Force retention enforcement
TimelessMetrics.enforce_retention(:metrics)
```

#### Info fields

| Field | Description |
|-------|-------------|
| `series_count` | Number of unique time series |
| `segment_count` | Number of compressed raw segments |
| `total_points` | Total data points stored |
| `raw_compressed_bytes` | Raw segment storage in bytes |
| `bytes_per_point` | Compression efficiency |
| `storage_bytes` | Total on-disk storage (segment files + metadata DB) |
| `oldest_timestamp` | Earliest data point |
| `newest_timestamp` | Latest data point |
| `buffer_points` | Points still in write buffers |
| `buffer_shards` | Number of buffer shards |
| `tiers` | Map of tier names to stats |
| `raw_retention` | Raw data retention in seconds |
| `db_path` | Main database file path |

---

## HTTP API

Start the HTTP server alongside TimelessMetrics:

```elixir
children = [
  {TimelessMetrics, name: :metrics, data_dir: "/var/lib/metrics"},
  {TimelessMetrics.HTTP, store: :metrics, port: 8428}
]
```

Or run the container (starts both automatically).

All ingest and query endpoints are compatible with VictoriaMetrics tooling
(Vector, Grafana, etc.).

### Authentication

Set the `TIMELESS_BEARER_TOKEN` environment variable to enable token authentication.
When set, all endpoints except `/health` require a valid token.

**Via header** (API clients, curl, Grafana):

```bash
curl -H "Authorization: Bearer my-secret-token" \
  http://localhost:8428/api/v1/query_range?metric=cpu_usage&from=-1h
```

**Via query parameter** (browsers, embedded charts):

```
http://localhost:8428/chart?metric=cpu_usage&from=-6h&token=my-secret-token
http://localhost:8428/?token=my-secret-token
```

**Elixir library usage** (pass `:bearer_token` in HTTP opts):

```elixir
{TimelessMetrics.HTTP, store: :metrics, port: 8428, bearer_token: "my-secret-token"}
```

| Response | Meaning |
|---|---|
| `401 {"error":"unauthorized"}` | No token provided (missing header and no `?token=` param) |
| `403 {"error":"forbidden"}` | Token provided but doesn't match |

When `TIMELESS_BEARER_TOKEN` is not set, all endpoints are open (no auth enforced).
`/health` is always open regardless of token configuration.

### Ingest Endpoints

#### `POST /api/v1/import`

VictoriaMetrics JSON line format. Each line is a JSON object:

```
{"metric":{"__name__":"cpu_usage","host":"web-1"},"values":[73.2,74.1],"timestamps":[1700000000,1700000060]}
{"metric":{"__name__":"mem_usage","host":"web-1"},"values":[4200000],"timestamps":[1700000000]}
```

- `metric.__name__` is the metric name; all other keys become labels
- `values` and `timestamps` are parallel arrays
- Max body size: 10 MB

**Response:**
- `204 No Content` on success
- `200` with `{"samples": N, "errors": N}` if some lines had errors
- `413` if body exceeds 10 MB

**Example:**

```bash
curl -X POST http://localhost:8428/api/v1/import -d '
{"metric":{"__name__":"cpu_usage","host":"web-1"},"values":[73.2],"timestamps":[1700000000]}
{"metric":{"__name__":"cpu_usage","host":"web-2"},"values":[81.0],"timestamps":[1700000000]}
'
```

**Vector sink configuration:**

```toml
[sinks.timeless]
type = "http"
inputs = ["metrics_transform"]
uri = "http://localhost:8428/api/v1/import"
encoding.codec = "text"
framing.method = "newline_delimited"
```

#### `POST /api/v1/import/prometheus`

Prometheus text exposition format:

```
cpu_usage{host="web-1"} 73.2 1700000000000
cpu_usage{host="web-2"} 81.0
mem_usage 4200000
```

- Lines starting with `#` are skipped (comments, HELP, TYPE)
- Timestamp is in milliseconds (converted to seconds internally)
- If timestamp is omitted, current time is used
- Labels are optional

**Example:**

```bash
curl -X POST http://localhost:8428/api/v1/import/prometheus -d '
# HELP cpu_usage CPU utilization
cpu_usage{host="web-1",region="us-east"} 73.2 1700000000000
cpu_usage{host="web-2",region="us-east"} 81.0 1700000000000
'
```

### Query Endpoints

All query endpoints accept these common parameters:

| Parameter | Description |
|-----------|-------------|
| `metric` | Metric name (**required**) |
| `from` or `start` | Start timestamp — unix seconds or relative (`-1h`, `-30m`, `-7d`) |
| `to` or `end` | End timestamp — unix seconds, relative, or `now` |
| Any other param | Treated as a label filter (e.g., `host=web-1`) |

Relative time syntax: `-<number><unit>` where unit is `s`, `m`, `h`, `d`, or `w`.

#### `GET /api/v1/export`

Export raw points in VictoriaMetrics JSON line format (one line per series).

```bash
curl 'http://localhost:8428/api/v1/export?metric=cpu_usage&host=web-1&from=-1h'
```

**Response** (newline-delimited JSON):

```json
{"metric":{"__name__":"cpu_usage","host":"web-1"},"values":[73.2,74.1],"timestamps":[1700000000,1700000060]}
```

#### `GET /api/v1/query`

Latest value for matching series.

```bash
curl 'http://localhost:8428/api/v1/query?metric=cpu_usage&host=web-1'
```

**Response** (single series):

```json
{"labels":{"host":"web-1"},"timestamp":1700000060,"value":74.1}
```

**Response** (multiple series):

```json
{"data":[
  {"labels":{"host":"web-1"},"timestamp":1700000060,"value":74.1},
  {"labels":{"host":"web-2"},"timestamp":1700000060,"value":81.0}
]}
```

#### `GET /api/v1/query_range`

Range query with time-bucketed aggregation.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `step` | `60` | Bucket size in seconds |
| `aggregate` | `avg` | One of: `avg`, `min`, `max`, `sum`, `count`, `last`, `first`, `rate` |

```bash
curl 'http://localhost:8428/api/v1/query_range?metric=cpu_usage&from=-6h&step=300&aggregate=max'
```

**Response:**

```json
{
  "metric": "cpu_usage",
  "series": [
    {
      "labels": {"host": "web-1"},
      "data": [[1700000000, 73.5], [1700000300, 91.2]]
    },
    {
      "labels": {"host": "web-2"},
      "data": [[1700000000, 81.0], [1700000300, 79.3]]
    }
  ]
}
```

### Prometheus-Compatible Endpoints

#### `GET /prometheus/api/v1/query_range`

Grafana-compatible Prometheus query_range endpoint. Supports simple PromQL
selectors (metric name with optional label matchers).

| Parameter | Description |
|-----------|-------------|
| `query` | PromQL selector, e.g. `cpu_usage{host="web-1"}` |
| `start` | Start timestamp (unix seconds, float) |
| `end` | End timestamp (unix seconds, float) |
| `step` | Step in seconds or duration string (`60s`, `5m`, `1h`) |

```bash
curl 'http://localhost:8428/prometheus/api/v1/query_range?query=cpu_usage{host="web-1"}&start=1700000000&end=1700003600&step=60'
```

**Response** (Prometheus API format):

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {"__name__": "cpu_usage", "host": "web-1"},
        "values": [[1700000000, "73.2"], [1700000060, "74.1"]]
      }
    ]
  }
}
```

**Grafana data source configuration:**

Set Prometheus URL to `http://timeless:8428/prometheus` and queries will work
with standard PromQL selectors.

### Series Discovery Endpoints

#### `GET /api/v1/label/__name__/values`

List all metric names.

```bash
curl http://localhost:8428/api/v1/label/__name__/values
```

```json
{"status":"success","data":["cpu_usage","mem_usage","disk_io"]}
```

#### `GET /api/v1/label/:name/values?metric=<metric>`

List distinct values for a label key.

```bash
curl 'http://localhost:8428/api/v1/label/host/values?metric=cpu_usage'
```

```json
{"status":"success","data":["web-1","web-2","web-3"]}
```

#### `GET /api/v1/series?metric=<metric>`

List all series (label combinations) for a metric.

```bash
curl 'http://localhost:8428/api/v1/series?metric=cpu_usage'
```

```json
{"status":"success","data":[{"labels":{"host":"web-1"}},{"labels":{"host":"web-2"}}]}
```

### Metadata Endpoints

#### `POST /api/v1/metadata`

Register or update metric metadata.

```bash
curl -X POST http://localhost:8428/api/v1/metadata -d '{
  "metric": "cpu_usage",
  "type": "gauge",
  "unit": "%",
  "description": "CPU utilization percentage"
}'
```

| Field | Required | Values |
|-------|----------|--------|
| `metric` | yes | Metric name |
| `type` | yes | `gauge`, `counter`, or `histogram` |
| `unit` | no | Unit string (e.g., `%`, `bytes`, `ms`) |
| `description` | no | Human-readable description |

#### `GET /api/v1/metadata?metric=<metric>`

Get metadata for a metric. Returns default values (type: gauge) if none registered.

```bash
curl 'http://localhost:8428/api/v1/metadata?metric=cpu_usage'
```

```json
{"metric":"cpu_usage","type":"gauge","unit":"%","description":"CPU utilization percentage"}
```

### Annotation Endpoints

Annotations are event markers (deploys, incidents, maintenance windows) that
overlay on charts as dashed vertical lines with labels.

#### `POST /api/v1/annotations`

```bash
curl -X POST http://localhost:8428/api/v1/annotations -d '{
  "title": "Deploy v2.1",
  "description": "Rolled out new caching layer",
  "tags": ["deploy", "prod"],
  "timestamp": 1700000000
}'
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `title` | yes | | Short annotation title |
| `description` | no | `null` | Longer description |
| `tags` | no | `[]` | List of tag strings for filtering |
| `timestamp` | no | current time | Unix seconds |

**Response:** `201 {"id": 1, "status": "created"}`

#### `GET /api/v1/annotations`

Query annotations in a time range.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `from` | 24h ago | Start timestamp |
| `to` | now | End timestamp |
| `tags` | (all) | Comma-separated tag filter (matches any) |

```bash
curl 'http://localhost:8428/api/v1/annotations?from=-7d&tags=deploy,incident'
```

```json
{
  "data": [
    {"id": 1, "timestamp": 1700000000, "title": "Deploy v2.1", "description": "...", "tags": ["deploy", "prod"]}
  ]
}
```

#### `DELETE /api/v1/annotations/:id`

```bash
curl -X DELETE http://localhost:8428/api/v1/annotations/1
```

### Alert Endpoints

#### `POST /api/v1/alerts`

Create an alert rule.

```bash
curl -X POST http://localhost:8428/api/v1/alerts -d '{
  "name": "High CPU",
  "metric": "cpu_usage",
  "condition": "above",
  "threshold": 90.0,
  "labels": {"host": "web-1"},
  "duration": 300,
  "aggregate": "avg",
  "webhook_url": "http://hooks.example.com/alert"
}'
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | yes | | Alert name |
| `metric` | yes | | Metric to monitor |
| `condition` | yes | | `above` or `below` |
| `threshold` | yes | | Numeric threshold |
| `labels` | no | `{}` | Label filter (empty = all series) |
| `duration` | no | `0` | Seconds value must breach before firing |
| `aggregate` | no | `avg` | Aggregate function for evaluation |
| `webhook_url` | no | `null` | URL to POST on state transitions |

**Response:** `201 {"id": 1, "status": "created"}`

#### `GET /api/v1/alerts`

List all alert rules with current state.

```bash
curl http://localhost:8428/api/v1/alerts
```

#### `DELETE /api/v1/alerts/:id`

```bash
curl -X DELETE http://localhost:8428/api/v1/alerts/1
```

### Charts and Dashboard

#### `GET /chart`

Render an SVG line chart. See [SVG Charts](#svg-charts) for full details.

#### `GET /`

Auto-generated HTML dashboard with all metrics, alert badges, and auto-refresh.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `from` | `-1h` | Time range start |
| `to` | `now` | Time range end |
| Any other param | | Label filter |

```
http://localhost:8428/?from=-6h&host=web-1
```

### Health Check

#### `GET /health`

```bash
curl http://localhost:8428/health
```

```json
{
  "status": "ok",
  "series": 1000,
  "points": 5000000,
  "storage_bytes": 4194304,
  "buffer_points": 1234,
  "bytes_per_point": 0.78
}
```

---

## SVG Charts

TimelessMetrics generates pure SVG line charts with no JavaScript or external
dependencies. Charts can be embedded anywhere that renders images: HTML `<img>`
tags, markdown, emails, Slack, notebooks, etc.

### Embedding Charts

#### HTML `<img>` tag

```html
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-6h" />
```

#### Markdown

```markdown
![CPU Usage](http://localhost:8428/chart?metric=cpu_usage&from=-6h&theme=light)
```

#### Multi-series comparison

```html
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-24h&step=300&aggregate=max&width=1000&height=400" />
```

All series matching the label filter appear as separate colored lines with an
auto-generated legend.

### Chart Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `metric` | **required** | Metric name |
| `from` / `start` | 1h ago | Start time (unix seconds or relative: `-1h`, `-7d`) |
| `to` / `end` | now | End time |
| `step` | auto | Bucket size in seconds. Auto-computed from range if omitted (~200 buckets) |
| `aggregate` | `avg` | Aggregation: `avg`, `min`, `max`, `sum`, `count`, `last`, `first`, `rate` |
| `width` | `800` | SVG width in pixels |
| `height` | `300` | SVG height in pixels |
| `theme` | `auto` | `light`, `dark`, or `auto` |
| Any other param | | Label filter (e.g., `host=web-1`) |

### Themes

| Theme | Behavior |
|-------|----------|
| `auto` | Uses CSS `prefers-color-scheme` media query. Renders correctly in both light and dark contexts without server-side knowledge of the viewer's preference. |
| `light` | White background, dark text |
| `dark` | Dark gray background, light text |

### Chart Features

- **Multi-series**: All matching series render as separate colored lines
  (up to 8 colors, cycling)
- **Auto-legend**: When multiple series are present, a legend appears at the
  bottom using the most-varying label key
- **Annotation markers**: Dashed amber vertical lines for any annotations in
  the time range, with title labels
- **Smart axes**: Y-axis uses "nice" tick values (1, 2, 5 multiples);
  X-axis snaps to clean time intervals
- **Value formatting**: Large values shown as `1.5K`, `2.3M`
- **Time formatting**: Adapts to range — `HH:MM` for <1 day, `Mon HH:MM`
  for <1 week, `M/D` for longer ranges
- **Empty state**: Renders a clean "No data" placeholder when no points match
- **Cache-friendly**: Responses include `Cache-Control: public, max-age=60`

### Programmatic Usage

The chart module can be used directly from Elixir without the HTTP server:

```elixir
{:ok, results} = TimelessMetrics.query_aggregate_multi(:metrics, "cpu_usage", %{},
  from: now - 3600,
  to: now,
  bucket: {60, :seconds},
  aggregate: :avg)

{:ok, annotations} = TimelessMetrics.annotations(:metrics, now - 3600, now)

svg = TimelessMetrics.Chart.render("cpu_usage", results,
  width: 800,
  height: 300,
  theme: :dark,
  annotations: annotations)

File.write!("chart.svg", svg)
```

---

## Configuration

### Store Options

When starting TimelessMetrics as a library:

```elixir
{TimelessMetrics,
  name: :metrics,
  data_dir: "/var/lib/metrics",
  buffer_shards: 8,
  flush_interval: :timer.seconds(5),
  flush_threshold: 10_000,
  segment_duration: 14_400,
  compression: :zstd,
  schema: MyApp.MetricsSchema}
```

### Schema and Rollup Tiers

Define custom rollup tiers:

```elixir
defmodule MyApp.MetricsSchema do
  use TimelessMetrics.Schema

  raw_retention {7, :days}
  rollup_interval {5, :minutes}
  retention_interval {1, :hours}

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

**Default schema** (used when no `:schema` option is provided):
- Raw retention: 7 days
- Hourly rollups retained 30 days
- Daily rollups retained 365 days
- Monthly rollups retained forever
- Rollup runs every 5 minutes
- Retention enforced every hour

### Environment Variables (Container)

When running as a standalone container, these environment variables configure
the store:

| Variable | Default | Description |
|----------|---------|-------------|
| `TIMELESS_DATA_DIR` | `/data` | Storage directory (mount a volume here) |
| `TIMELESS_PORT` | `8428` | HTTP listen port |
| `TIMELESS_SHARDS` | `schedulers / 2` | Number of write buffer/builder shards |
| `TIMELESS_SEGMENT_DURATION` | `14400` | Raw segment duration in seconds |

```bash
podman run -d \
  -p 8428:8428 \
  -v timeless_data:/data:Z \
  -e TIMELESS_SHARDS=4 \
  -e TIMELESS_SEGMENT_DURATION=7200 \
  localhost/timeless:latest
```
