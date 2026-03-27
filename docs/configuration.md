# Configuration Reference

## Store options

All options are passed to the `TimelessMetrics` child spec:

```elixir
{TimelessMetrics,
  name: :metrics,
  data_dir: "/var/lib/metrics",
  schema: MyApp.MetricsSchema,
  raw_retention_seconds: 14 * 86_400,
  daily_retention_seconds: 365 * 86_400,
  ingest_workers: 8,
  alert_interval: :timer.seconds(30),
  self_monitor: true,
  self_monitor_labels: %{"service" => "api"},
  scraping: true}
```

### Core options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `name` | `atom` | **(required)** | Store name used to reference this instance in all API calls |
| `data_dir` | `String.t()` | `"data"` | Directory for the store's SQLite admin data and Rust engine data files |
| `mode` | `:disk \| :memory` | `:disk` | `:disk` persists data under `data_dir`; `:memory` keeps the store ephemeral |
| `schema` | module or `%TimelessMetrics.Schema{}` | `TimelessMetrics.Schema.default()` | Rollup tier configuration |
| `raw_retention_seconds` | `pos_integer()` | `604_800` | How long raw point data is retained (7 days by default) |
| `daily_retention_seconds` | `pos_integer()` | `31_536_000` | How long daily rollup data is retained (365 days by default) |
| `ingest_workers` | `pos_integer()` | `max(div(schedulers, 4), 2)` | Background workers that drain HTTP ingest queues |
| `alert_interval` | `pos_integer()` | `60_000` | Milliseconds between alert evaluation cycles |
| `self_monitor` | `boolean()` | `true` | Enable internal self-monitoring metrics |
| `self_monitor_labels` | `map()` | `%{}` | Labels applied to self-monitoring metrics |
| `scraping` | `boolean()` | `true` | Enable the Prometheus scraping subsystem |
| `engine` | `:rust \| :legacy` | `:rust` | Engine selection. `:rust` is the default and maintained path; `:legacy` exists only for compatibility and migration work |

### Legacy-only options

These options apply only when `engine: :legacy` is used:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `buffer_shards` | `pos_integer()` | `max(div(schedulers, 2), 2)` | Number of ETS buffer shards |
| `flush_interval` | `pos_integer()` | `5_000` | Automatic buffer flush interval in milliseconds |
| `flush_threshold` | `pos_integer()` | `10_000` | Points per shard before immediate flush |
| `segment_duration` | `pos_integer()` | `14_400` | Segment window size in seconds |
| `compression` | `:zstd` | `:zstd` | Segment compression container |
| `compression_level` | `pos_integer()` | `2` | Zstd level for legacy segment files |

For new deployments, do not tune these unless you are intentionally running the legacy engine.

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
| `bearer_token` | `String.t() \| nil` | `nil` | Bearer token for API authentication. When `nil`, endpoints are open |

## Full configuration example

```elixir
# config/config.exs
config :my_app, :metrics,
  name: :metrics,
  data_dir: "/var/lib/my_app/metrics",
  raw_retention_seconds: 14 * 86_400,
  daily_retention_seconds: 2 * 365 * 86_400,
  ingest_workers: 8,
  alert_interval: :timer.seconds(30),
  self_monitor: true,
  self_monitor_labels: %{"service" => "api"},
  scraping: true
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

### `mode`

- Use `:disk` for normal deployments. This is the durable path.
- Use `:memory` for tests, short-lived benchmarks, and ephemeral experimentation.

### `ingest_workers`

These workers drain the HTTP import queue. They matter only for the HTTP ingest path, not direct Elixir writes.

- Default: good for most deployments
- Increase: when HTTP import is saturated and CPU is available
- Decrease: for small embedded deployments where background concurrency is unnecessary

### `raw_retention_seconds` and `daily_retention_seconds`

- Raw retention controls how long full-resolution point data stays queryable.
- Daily retention controls long-term rollup retention.
- Longer retention improves historical visibility but increases disk usage.

### `schema`

Use a custom schema when your retention tiers need to match your dashboarding horizon.

- Keep raw data shorter for high-volume operational metrics.
- Extend daily or monthly tiers for long-term planning and reporting.
- Treat schema changes as storage-layout decisions, not cosmetic config.

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
