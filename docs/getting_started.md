# Getting Started

This guide walks you through installing TimelessMetrics, writing your first metric, and querying it back.

## Installation

### As a library

Add to your `mix.exs`:

```elixir
def deps do
  [
    {:timeless_metrics, "~> 1.2"}
  ]
end
```

Then fetch dependencies:

```bash
mix deps.get
```

### As a container

```bash
podman build -f Containerfile -t timeless_metrics:latest .
podman run -d -p 8428:8428 -v timeless_data:/data:Z localhost/timeless_metrics:latest
```

## Minimal configuration

TimelessMetrics requires two options: a name and a data directory.

```elixir
children = [
  {TimelessMetrics, name: :metrics, data_dir: "/var/lib/metrics"}
]
```

To expose the HTTP API, add the HTTP server:

```elixir
children = [
  {TimelessMetrics, name: :metrics, data_dir: "/var/lib/metrics"},
  {TimelessMetrics.HTTP, store: :metrics, port: 8428}
]
```

Add these to your application's supervision tree (e.g., in `lib/my_app/application.ex`):

```elixir
defmodule MyApp.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {TimelessMetrics, name: :metrics, data_dir: "/var/lib/metrics"},
      {TimelessMetrics.HTTP, store: :metrics, port: 8428}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
  end
end
```

## Writing your first metric

```elixir
# Write a single data point
TimelessMetrics.write(:metrics, "cpu_usage", %{"host" => "web-1"}, 73.2)

# Write with an explicit timestamp (unix seconds)
TimelessMetrics.write(:metrics, "cpu_usage", %{"host" => "web-1"}, 74.1,
  timestamp: 1700000060)

# Batch write multiple points
TimelessMetrics.write_batch(:metrics, [
  {"cpu_usage", %{"host" => "web-1"}, 73.2},
  {"cpu_usage", %{"host" => "web-2"}, 81.0},
  {"mem_usage", %{"host" => "web-1"}, 4096.0}
])
```

Or via HTTP:

```bash
# VictoriaMetrics JSON line format
curl -X POST http://localhost:8428/api/v1/import -d \
  '{"metric":{"__name__":"cpu_usage","host":"web-1"},"values":[73.2],"timestamps":[1700000000]}'

# Prometheus text format
curl -X POST http://localhost:8428/api/v1/import/prometheus -d \
  'cpu_usage{host="web-1"} 73.2'
```

## Querying it back

### Elixir API

```elixir
# Raw points for the last hour
{:ok, points} = TimelessMetrics.query(:metrics, "cpu_usage", %{"host" => "web-1"},
  from: System.os_time(:second) - 3600)
# => {:ok, [{1700000000, 73.2}, {1700000060, 74.1}, ...]}

# Aggregated: 1-minute averages over the last hour
{:ok, buckets} = TimelessMetrics.query_aggregate(:metrics, "cpu_usage", %{"host" => "web-1"},
  from: System.os_time(:second) - 3600,
  bucket: :minute,
  aggregate: :avg)
# => {:ok, [{1700000000, 73.65}, {1700000060, 74.1}, ...]}

# Latest value
{:ok, latest} = TimelessMetrics.latest(:metrics, "cpu_usage", %{"host" => "web-1"})
# => {:ok, {1700000060, 74.1}}
```

### HTTP API

```bash
# Range query with 60-second buckets
curl 'http://localhost:8428/api/v1/query_range?metric=cpu_usage&host=web-1&from=-1h&step=60'

# Latest value
curl 'http://localhost:8428/api/v1/query?metric=cpu_usage&host=web-1'

# Export raw points
curl 'http://localhost:8428/api/v1/export?metric=cpu_usage&host=web-1&from=-1h'
```

## Viewing the dashboard

Open `http://localhost:8428/` in your browser for a built-in HTML dashboard showing all metrics with auto-refresh.

## Embedding a chart

```html
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-6h&theme=auto" />
```

## Next steps

- [Configuration Reference](configuration.md) -- all supervisor options and tuning guidance
- [Architecture](architecture.md) -- how the storage engine works
- [API Reference](API.md) -- complete Elixir and HTTP API documentation
- [Scraping](scraping.md) -- pull metrics from Prometheus endpoints
- [Charts & Embedding](charts.md) -- SVG chart generation and embedding
- [Operations](operations.md) -- monitoring, backup, and troubleshooting
- Interactive exploration: run the User's Guide livebook at `livebook/users_guide.livemd`
