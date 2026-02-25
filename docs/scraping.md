# Scraping

TimelessMetrics includes a built-in Prometheus-compatible scraper that polls `/metrics` endpoints on a configurable interval. This lets you pull metrics from any application that exposes a Prometheus text exposition endpoint.

## How scraping works

Each scrape target is a background worker process that:

1. Sends an HTTP GET to the target's metrics endpoint
2. Parses the Prometheus text exposition response
3. Applies relabel and metric relabel configs (if any)
4. Writes the parsed metrics into the store via `write_batch`
5. Updates scrape health tracking (up/down, duration, sample count)
6. Auto-registers metric metadata (type and unit inferred from metric name suffixes)

Workers use anchor-based scheduling to avoid clock drift, with initial jitter to spread load across targets.

## Elixir API

The scraper process is named `:"#{store_name}_scraper"`. You can interact with it directly or through the HTTP API.

```elixir
# The scraper name for a store called :metrics
scraper = :metrics_scraper

# Add a target
{:ok, id} = TimelessMetrics.Scraper.add_target(scraper, %{
  "job_name" => "my_app",
  "address" => "localhost:4000",
  "metrics_path" => "/metrics",
  "scrape_interval" => 30
})

# List all targets with health status
{:ok, targets} = TimelessMetrics.Scraper.list_targets(scraper)

# Get a single target
{:ok, target} = TimelessMetrics.Scraper.get_target(scraper, id)

# Update a target
:ok = TimelessMetrics.Scraper.update_target(scraper, id, %{
  "scrape_interval" => 15
})

# Delete a target
:ok = TimelessMetrics.Scraper.delete_target(scraper, id)
```

## HTTP API

### Create a scrape target

```bash
curl -X POST http://localhost:8428/api/v1/scrape_targets \
  -H 'Content-Type: application/json' \
  -d '{
    "job_name": "my_phoenix_app",
    "address": "localhost:4000",
    "metrics_path": "/metrics",
    "scrape_interval": 30
  }'
# => {"id": 1, "status": "created"}
```

### List all targets

```bash
curl http://localhost:8428/api/v1/scrape_targets
```

Response includes health information for each target:

```json
{
  "data": [
    {
      "id": 1,
      "job_name": "my_phoenix_app",
      "address": "localhost:4000",
      "metrics_path": "/metrics",
      "scrape_interval": 30,
      "enabled": true,
      "health": {
        "health": "up",
        "last_scrape": 1700000060,
        "last_duration_ms": 12,
        "last_error": null,
        "samples_scraped": 42
      }
    }
  ]
}
```

### Get a single target

```bash
curl http://localhost:8428/api/v1/scrape_targets/1
```

### Update a target

```bash
curl -X PUT http://localhost:8428/api/v1/scrape_targets/1 \
  -H 'Content-Type: application/json' \
  -d '{"job_name": "my_phoenix_app", "address": "localhost:4000", "scrape_interval": 15}'
```

### Delete a target

```bash
curl -X DELETE http://localhost:8428/api/v1/scrape_targets/1
```

## Target options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `job_name` | string | **(required)** | Identifies this scrape job (added as `job` label) |
| `address` | string | **(required)** | Host and port to scrape (e.g., `"localhost:4000"`) |
| `scheme` | string | `"http"` | URL scheme (`"http"` or `"https"`) |
| `metrics_path` | string | `"/metrics"` | Path to the metrics endpoint |
| `scrape_interval` | integer | `30` | Seconds between scrapes |
| `scrape_timeout` | integer | `10` | Seconds before a scrape request times out |
| `labels` | map | `{}` | Extra labels added to all scraped metrics |
| `honor_labels` | boolean | `false` | If true, scraped labels take precedence over target labels on conflict |
| `honor_timestamps` | boolean | `true` | If true, use timestamps from the scraped response; if false, use scrape time |
| `relabel_configs` | list | `null` | Relabel configs applied before scraping (see below) |
| `metric_relabel_configs` | list | `null` | Relabel configs applied after scraping (see below) |
| `enabled` | boolean | `true` | Whether this target is actively scraped |

## Relabel configs

Relabel configs transform labels using regex-based rules, following the Prometheus relabeling model. Each config is a JSON object with these fields:

| Field | Default | Description |
|-------|---------|-------------|
| `source_labels` | `["__name__"]` | Label keys to concatenate (joined by `;`) as the input |
| `separator` | `";"` | Separator for joining source labels |
| `regex` | `".*"` | Regex to match against the concatenated source labels |
| `target_label` | | Label key to write the result to |
| `replacement` | `"$1"` | Replacement string (supports regex capture groups) |
| `action` | `"replace"` | Action: `replace`, `keep`, `drop`, `labelmap`, `labeldrop`, `labelkeep` |

### Metric relabel configs

Applied after scraping, these let you filter or transform metrics before they're stored. Common uses:

**Drop unwanted metrics:**

```json
{
  "metric_relabel_configs": [
    {
      "source_labels": ["__name__"],
      "regex": "go_.*",
      "action": "drop"
    }
  ]
}
```

**Keep only specific metrics:**

```json
{
  "metric_relabel_configs": [
    {
      "source_labels": ["__name__"],
      "regex": "http_requests_total|http_request_duration_seconds.*",
      "action": "keep"
    }
  ]
}
```

**Rename a metric:**

```json
{
  "metric_relabel_configs": [
    {
      "source_labels": ["__name__"],
      "regex": "old_metric_name",
      "target_label": "__name__",
      "replacement": "new_metric_name"
    }
  ]
}
```

## Health monitoring

Each scrape worker tracks health status and writes self-monitoring metrics:

| Metric | Description |
|--------|-------------|
| `up` | `1` if the last scrape succeeded, `0` if it failed |
| `scrape_duration_seconds` | Duration of the last scrape in seconds |
| `scrape_samples_scraped` | Number of samples parsed from the last scrape |

These metrics have `job` and `instance` labels matching the target's `job_name` and `address`.

Health status is also stored in the `scrape_health` SQLite table and returned in the target listing API.

## Auto-registered metadata

When new metric names are discovered during scraping, the worker automatically registers metadata based on Prometheus naming conventions:

| Suffix | Inferred type | Inferred unit |
|--------|--------------|---------------|
| `_bytes` | gauge | byte |
| `_seconds` | gauge | second |
| `_milliseconds` | gauge | millisecond |
| `_total` | counter | -- |
| `_count` | counter | -- |
| `_ratio` | gauge | ratio |
| `_percent` | gauge | percent |
| *(other)* | gauge | -- |

## Example: scraping a Phoenix app

A Phoenix app using `:telemetry_metrics_prometheus_core` exposes metrics at `/metrics`:

```bash
curl -X POST http://localhost:8428/api/v1/scrape_targets \
  -H 'Content-Type: application/json' \
  -d '{
    "job_name": "phoenix_app",
    "address": "localhost:4000",
    "metrics_path": "/metrics",
    "scrape_interval": 15,
    "labels": {"env": "production"},
    "metric_relabel_configs": [
      {"source_labels": ["__name__"], "regex": "go_.*", "action": "drop"}
    ]
  }'
```

## Example: scraping Node Exporter

```bash
curl -X POST http://localhost:8428/api/v1/scrape_targets \
  -H 'Content-Type: application/json' \
  -d '{
    "job_name": "node_exporter",
    "address": "localhost:9100",
    "scrape_interval": 30,
    "labels": {"host": "web-1"}
  }'
```

## Disabling scraping

If you don't need scraping, disable it to save resources:

```elixir
{TimelessMetrics, name: :metrics, data_dir: "/data", scraping: false}
```
