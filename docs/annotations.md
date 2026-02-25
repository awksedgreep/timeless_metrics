# Annotations

Annotations are event markers that record when something notable happened -- deployments, incidents, maintenance windows, config changes. They appear as vertical dashed lines on SVG charts and can be queried independently.

## What annotations store

Each annotation has:

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Auto-generated unique ID |
| `timestamp` | integer | Unix timestamp (seconds) when the event occurred |
| `title` | string | Short description (e.g., "Deploy v2.3.1") |
| `description` | string | Optional longer description |
| `tags` | list | Optional tags for filtering (e.g., `["deploy", "backend"]`) |

## Elixir API

### Create an annotation

```elixir
{:ok, id} = TimelessMetrics.annotate(:metrics, System.os_time(:second), "Deploy v2.3.1",
  description: "Rolled out new caching layer",
  tags: ["deploy", "backend"])
# => {:ok, 1}
```

### Query annotations

```elixir
now = System.os_time(:second)

# All annotations in the last 24 hours
{:ok, annotations} = TimelessMetrics.annotations(:metrics, now - 86_400, now)

# Filter by tags
{:ok, deploys} = TimelessMetrics.annotations(:metrics, now - 86_400, now,
  tags: ["deploy"])
```

Returns:

```elixir
{:ok, [
  %{id: 1, timestamp: 1700000000, title: "Deploy v2.3.1",
    description: "Rolled out new caching layer", tags: ["deploy", "backend"]},
  %{id: 2, timestamp: 1700043200, title: "Deploy v2.3.2",
    description: nil, tags: ["deploy"]}
]}
```

### Delete an annotation

```elixir
:ok = TimelessMetrics.delete_annotation(:metrics, 1)
```

## HTTP API

### Create an annotation

```bash
curl -X POST http://localhost:8428/api/v1/annotations \
  -H 'Content-Type: application/json' \
  -d '{
    "title": "Deploy v2.3.1",
    "description": "Rolled out new caching layer",
    "tags": ["deploy", "backend"],
    "timestamp": 1700000000
  }'
# => {"id": 1, "status": "created"}
```

If `timestamp` is omitted, the current time is used.

### Query annotations

```bash
# All annotations in the last 24 hours
curl 'http://localhost:8428/api/v1/annotations?from=-24h&to=now'

# Filter by tags (comma-separated)
curl 'http://localhost:8428/api/v1/annotations?from=-7d&tags=deploy,incident'
```

Response:

```json
{
  "data": [
    {
      "id": 1,
      "timestamp": 1700000000,
      "title": "Deploy v2.3.1",
      "description": "Rolled out new caching layer",
      "tags": ["deploy", "backend"]
    }
  ]
}
```

### Delete an annotation

```bash
curl -X DELETE http://localhost:8428/api/v1/annotations/1
# => {"status": "deleted"}
```

## Tag-based filtering

Tags let you categorize annotations and query specific types:

```elixir
# Mark different event types
TimelessMetrics.annotate(:metrics, now, "Deploy v2.3.1", tags: ["deploy"])
TimelessMetrics.annotate(:metrics, now, "High error rate", tags: ["incident", "p1"])
TimelessMetrics.annotate(:metrics, now, "DB maintenance", tags: ["maintenance"])

# Query only incidents
{:ok, incidents} = TimelessMetrics.annotations(:metrics, from, to, tags: ["incident"])

# Query deploys and maintenance
{:ok, events} = TimelessMetrics.annotations(:metrics, from, to, tags: ["deploy", "maintenance"])
```

When filtering by multiple tags, annotations matching **any** of the specified tags are returned (OR logic).

## Chart overlay

Annotations automatically appear on SVG charts rendered via the `/chart` endpoint. They display as vertical amber dashed lines with the title text:

```html
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-24h" />
```

Any annotations within the chart's time range are included automatically. This makes it easy to correlate metric changes with events like deployments.

See [Charts & Embedding](charts.md) for more chart options.

## Common patterns

### Marking deployments from CI

```bash
# In your CI/CD pipeline
curl -X POST http://localhost:8428/api/v1/annotations \
  -H "Authorization: Bearer $METRICS_TOKEN" \
  -d "{
    \"title\": \"Deploy ${CI_COMMIT_SHORT_SHA}\",
    \"description\": \"Branch: ${CI_COMMIT_BRANCH}, Pipeline: ${CI_PIPELINE_ID}\",
    \"tags\": [\"deploy\", \"${CI_ENVIRONMENT_NAME}\"]
  }"
```

### Marking incidents

```elixir
TimelessMetrics.annotate(:metrics, System.os_time(:second), "P1: Payment failures",
  description: "Error rate spike in payment service, investigating",
  tags: ["incident", "p1", "payments"])
```

### Marking maintenance windows

```elixir
# Start of maintenance
{:ok, start_id} = TimelessMetrics.annotate(:metrics, System.os_time(:second),
  "Maintenance start: DB migration",
  tags: ["maintenance"])

# ... perform maintenance ...

# End of maintenance
TimelessMetrics.annotate(:metrics, System.os_time(:second),
  "Maintenance end: DB migration complete",
  tags: ["maintenance"])
```
