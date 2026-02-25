# Charts & Embedding

TimelessMetrics generates pure-Elixir SVG line charts with no external dependencies. Charts are embeddable via `<img>` tags in HTML, markdown, emails, Slack, and notebooks.

## Quick start

```html
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-6h" />
```

## The `/chart` endpoint

`GET /chart` returns an SVG image based on query parameters.

### Query parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `metric` | string | **(required)** | Metric name to chart |
| `from` | string/int | `-1h` | Start time (unix seconds or relative: `-1h`, `-6h`, `-24h`, `-7d`) |
| `to` | string/int | `now` | End time |
| `step` | int | auto | Bucket size in seconds. Auto-computed from time range if omitted (range / 200) |
| `aggregate` | string | `avg` | Aggregate function: `avg`, `min`, `max`, `sum`, `count`, `last`, `first`, `rate` |
| `width` | int | `800` | SVG width in pixels |
| `height` | int | `300` | SVG height in pixels |
| `theme` | string | `auto` | Color theme: `dark`, `light`, or `auto` |
| `forecast` | string | -- | Forecast horizon (e.g., `1h`, `6h`, `1d`). Adds a purple dashed forecast line |
| `anomalies` | string | -- | Anomaly sensitivity: `low`, `medium`, or `high`. Adds red anomaly dots |
| `transform` | string | -- | Optional data transform (e.g., `rate`) |
| `token` | string | -- | Bearer token for authenticated access (alternative to Authorization header) |

Any other query parameter becomes a label filter (e.g., `&host=web-1`).

### Examples

```bash
# Basic chart: last 6 hours of CPU usage
curl 'http://localhost:8428/chart?metric=cpu_usage&from=-6h' -o chart.svg

# Multi-series: all hosts (each series gets a different color)
curl 'http://localhost:8428/chart?metric=cpu_usage&from=-6h' -o chart.svg

# Single host with max aggregation
curl 'http://localhost:8428/chart?metric=cpu_usage&host=web-1&from=-24h&aggregate=max' -o chart.svg

# Custom dimensions
curl 'http://localhost:8428/chart?metric=cpu_usage&from=-6h&width=1200&height=400' -o chart.svg

# With forecast and anomaly overlays
curl 'http://localhost:8428/chart?metric=cpu_usage&from=-24h&forecast=6h&anomalies=medium' -o chart.svg
```

## Themes

| Theme | Description |
|-------|-------------|
| `light` | White background, dark text and grid lines |
| `dark` | Dark gray background (`#1f2937`), light text and grid lines |
| `auto` | Uses CSS `prefers-color-scheme` media query to switch between light and dark at render time |

The `auto` theme embeds a `<style>` block with CSS classes, so the chart adapts to the user's system preference. This is the default.

```html
<!-- Adapts to user's system dark/light mode -->
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-6h&theme=auto" />

<!-- Always dark -->
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-6h&theme=dark" />
```

## Embedding in HTML

Charts are standard SVG images and work anywhere `<img>` tags are supported:

```html
<!-- Simple embed -->
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-6h" alt="CPU Usage" />

<!-- Responsive width -->
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-6h&width=1200"
     style="max-width: 100%; height: auto;" alt="CPU Usage" />

<!-- Auto-refresh with meta tag -->
<meta http-equiv="refresh" content="60">
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-6h" />
```

## Embedding in Slack

Slack renders images from URLs. Use an incoming webhook or bot to post a chart URL:

```bash
curl -X POST https://hooks.slack.com/services/T.../B.../... \
  -H 'Content-Type: application/json' \
  -d '{
    "text": "CPU Usage (last 6h)",
    "attachments": [{
      "image_url": "http://your-host:8428/chart?metric=cpu_usage&from=-6h&theme=light&width=600&height=200"
    }]
  }'
```

Note: Slack needs the chart URL to be publicly accessible or accessible from Slack's servers.

## Embedding in email

Most email clients support inline SVG via `<img>` tags with external URLs:

```html
<img src="http://your-host:8428/chart?metric=cpu_usage&from=-6h&theme=light"
     alt="CPU Usage" width="800" height="300" />
```

For emails, prefer the `light` theme since most email clients have white backgrounds.

## Embedding in dashboards

Charts include a `Cache-Control: public, max-age=60` header, so they refresh every minute when used in dashboards:

```html
<!DOCTYPE html>
<html>
<head>
  <meta http-equiv="refresh" content="60">
  <title>Metrics Dashboard</title>
</head>
<body>
  <h2>System Overview</h2>
  <img src="/chart?metric=cpu_usage&from=-6h" />
  <img src="/chart?metric=mem_usage&from=-6h" />
  <img src="/chart?metric=disk_io_bytes&from=-6h&aggregate=sum" />
</body>
</html>
```

## Programmatic usage

Generate charts from Elixir code using `TimelessMetrics.Chart.render/3`:

```elixir
data = [
  %{labels: %{"host" => "web-1"}, data: [{1700000000, 73.2}, {1700000060, 74.1}]},
  %{labels: %{"host" => "web-2"}, data: [{1700000000, 81.0}, {1700000060, 79.5}]}
]

svg = TimelessMetrics.Chart.render("cpu_usage", data,
  width: 800,
  height: 300,
  theme: :dark,
  annotations: [%{timestamp: 1700000030, title: "Deploy"}],
  forecast: [{1700000120, 75.0}, {1700000180, 76.2}],
  anomalies: [{1700000000, 73.2}])
```

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:width` | integer | `800` | SVG width in pixels |
| `:height` | integer | `300` | SVG height in pixels |
| `:theme` | atom | `:auto` | `:light`, `:dark`, or `:auto` |
| `:label_key` | string | auto-detected | Label key to use for the legend |
| `:annotations` | list | `[]` | Annotation maps with `:timestamp` and `:title` |
| `:forecast` | list | `[]` | Forecast points as `{timestamp, value}` tuples |
| `:anomalies` | list | `[]` | Anomaly points as `{timestamp, value}` tuples |

## Chart features

- **Multi-series**: up to 8 colors, with automatic legend showing the most distinctive label
- **Auto-scaling**: Y-axis with "nice" tick intervals, X-axis with appropriate time formatting
- **Time formatting**: adapts to range (HH:MM for < 1 day, day names for < 1 week, M/D for longer)
- **Value formatting**: K/M suffixes for large numbers
- **Annotations**: amber dashed vertical lines with title text
- **Forecast overlay**: purple dashed line extending beyond the data
- **Anomaly markers**: red dots on flagged data points
- **Empty state**: displays "No data" when no matching series are found

## Built-in dashboard

`GET /` serves a zero-dependency HTML dashboard that lists all metrics and embeds charts for each:

```bash
# View at http://localhost:8428/
curl http://localhost:8428/
```

The dashboard supports query parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `from` | `-1h` | Time range start |
| `to` | `now` | Time range end |

Label filters are also supported (e.g., `/?from=-6h&host=web-1`).

## Authentication for embedded charts

When bearer token authentication is enabled, charts can authenticate via the `token` query parameter instead of the Authorization header:

```html
<!-- Token in query param for browser/image access -->
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-6h&token=my-secret-token" />
```

The `/health` endpoint is always accessible without authentication.
