# Configuration Reference

## Supervisor options

All options are passed to the `TimelessMetrics` child spec:

```elixir
{TimelessMetrics,
  name: :metrics,
  data_dir: "/var/lib/metrics",
  compression: :zstd,
  max_blocks: 100,
  block_size: 1000,
  flush_interval: 60_000,
  raw_retention_seconds: 604_800,
  daily_retention_seconds: 31_536_000,
  rollup_interval: 300_000,
  retention_interval: 3_600_000,
  alert_interval: 60_000,
  self_monitor: true,
  scraping: true}
```

### Complete options table

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `name` | `atom` | **(required)** | Store name used to reference this instance in all API calls |
| `data_dir` | `String.t()` | **(required)** | Directory for SQLite databases and block data files |
| `compression` | `:zstd` | `:zstd` | Compression algorithm for stored blocks |
| `max_blocks` | `pos_integer()` | `100` | Maximum number of compressed blocks per series (ring buffer) |
| `block_size` | `pos_integer()` | `1000` | Points per compressed block before flushing |
| `flush_interval` | `pos_integer()` | `60_000` | Milliseconds between automatic buffer flushes |
| `raw_retention_seconds` | `pos_integer()` | `604_800` | How long to keep raw data (default: 7 days) |
| `daily_retention_seconds` | `pos_integer()` | `31_536_000` | How long to keep daily rollup data (default: 365 days) |
| `rollup_interval` | `pos_integer()` | `300_000` | Milliseconds between automatic rollup runs (default: 5 minutes) |
| `retention_interval` | `pos_integer()` | `3_600_000` | Milliseconds between retention enforcement runs (default: 1 hour) |
| `alert_interval` | `pos_integer()` | `60_000` | Milliseconds between alert evaluation cycles (default: 60 seconds) |
| `self_monitor` | `boolean()` | `true` | Enable self-monitoring (writes internal metrics about the store) |
| `scraping` | `boolean()` | `true` | Enable the Prometheus scraping subsystem |

## HTTP server options

```elixir
{TimelessMetrics.HTTP,
  store: :metrics,
  port: 8428,
  bearer_token: "my-secret-token"}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `store` | `atom` | **(required)** | Name of the TimelessMetrics store to serve |
| `port` | `pos_integer()` | `8428` | HTTP listen port |
| `bearer_token` | `String.t() \| nil` | `nil` | Bearer token for API authentication. When `nil`, all endpoints are open |

## Full configuration example

```elixir
# config/config.exs
config :my_app, :metrics,
  name: :metrics,
  data_dir: "/var/lib/my_app/metrics",
  max_blocks: 200,
  block_size: 2000,
  flush_interval: 30_000,
  raw_retention_seconds: 14 * 86_400,
  daily_retention_seconds: 2 * 365 * 86_400
```

```elixir
# lib/my_app/application.ex
defmodule MyApp.Application do
  use Application

  @impl true
  def start(_type, _args) do
    metrics_opts = Application.get_env(:my_app, :metrics)

    children = [
      {TimelessMetrics, metrics_opts},
      {TimelessMetrics.HTTP,
        store: metrics_opts[:name],
        port: 8428,
        bearer_token: System.get_env("METRICS_TOKEN")}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
  end
end
```

## Container environment variables

When running as a container, these environment variables configure the instance:

| Variable | Default | Description |
|----------|---------|-------------|
| `TIMELESS_DATA_DIR` | `/data` | Storage directory |
| `TIMELESS_PORT` | `8428` | HTTP listen port |
| `TIMELESS_BEARER_TOKEN` | *(none)* | Bearer token for API auth (unset = no auth) |

## Tuning guidance

### `block_size`

Controls how many points accumulate in the raw buffer before being compressed into a block. Larger blocks compress better but use more memory per series and increase write latency during compression.

- **1000 (default)**: good balance for most workloads
- **500**: lower latency flushes, slightly worse compression
- **2000-5000**: better compression for high-volume series

### `max_blocks`

The ring buffer size per series. When exceeded, the oldest block is evicted. Combined with `block_size`, this determines how much raw data each series holds in memory.

Total in-memory points per series = `max_blocks * block_size`

- **100 (default)**: 100K points per series (at 5s intervals, ~5.8 days)
- **200+**: for longer raw retention windows

### `flush_interval`

How often buffered data is flushed to compressed blocks. Shorter intervals reduce data loss risk on crashes but increase I/O.

- **60_000 (default)**: flush every minute
- **30_000**: for lower data loss tolerance
- **120_000**: for write-heavy workloads where batching helps

### `raw_retention_seconds` / `daily_retention_seconds`

Configure how long data is kept at each resolution tier:

- **Raw**: detailed point-level data. Default 7 days. Increase for operational dashboards that need fine granularity over longer periods.
- **Daily**: aggregated daily rollups (avg, min, max, sum, count, last). Default 365 days. Increase for long-term capacity planning.

### Multiple store instances

You can run multiple independent stores in the same application:

```elixir
children = [
  {TimelessMetrics, name: :app_metrics, data_dir: "/data/app"},
  {TimelessMetrics, name: :infra_metrics, data_dir: "/data/infra"},
  {TimelessMetrics.HTTP, store: :app_metrics, port: 8428},
  {TimelessMetrics.HTTP, store: :infra_metrics, port: 8429}
]
```
