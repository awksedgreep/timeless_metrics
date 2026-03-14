# Configuration Reference

## Supervisor options

All options are passed to the `TimelessMetrics` child spec:

```elixir
{TimelessMetrics,
  name: :metrics,
  data_dir: "/var/lib/metrics",
  buffer_shards: 48,
  flush_interval: 5_000,
  flush_threshold: 10_000,
  segment_duration: 14_400,
  compression: :zstd,
  compression_level: 2,
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
| `data_dir` | `String.t()` | **(required)** | Directory for SQLite databases and segment data files |
| `buffer_shards` | `pos_integer()` | `schedulers / 2` | Number of ETS write buffer shards (and paired SegmentBuilder workers) |
| `flush_interval` | `pos_integer()` | `5_000` | Milliseconds between automatic buffer flushes to SegmentBuilder |
| `flush_threshold` | `pos_integer()` | `10_000` | Points per shard before triggering an immediate flush |
| `segment_duration` | `pos_integer()` | `14_400` | Seconds per time window for segment files (default: 4 hours) |
| `compression` | `:zstd` | `:zstd` | Compression algorithm for stored segments |
| `compression_level` | `pos_integer()` | `2` | Zstd compression level (1-19). ALP encoding does the heavy lifting; higher zstd levels add minimal benefit |
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
  buffer_shards: 16,
  flush_threshold: 20_000,
  compression_level: 2,
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

### `buffer_shards`

Number of independent ETS write buffer + SegmentBuilder pairs. More shards = more write parallelism but more file descriptors and compression threads.

- **Default (`schedulers / 2`)**: good for most workloads. On a 96-core machine this is 48 shards.
- **Lower (4-8)**: for embedded/low-core deployments
- **Higher**: rarely needed, the default scales well

### `flush_threshold`

Points per shard before triggering an immediate flush to the SegmentBuilder. Lower values reduce data-in-buffer time, higher values reduce flush overhead.

- **10,000 (default)**: good balance
- **5,000**: for lower-latency query visibility
- **50,000+**: for very high throughput write-heavy workloads

### `flush_interval`

How often each buffer shard flushes to its SegmentBuilder regardless of point count. Acts as a ceiling on data visibility latency.

- **5,000 (default)**: data queryable within 5 seconds
- **1,000**: near-real-time visibility
- **30,000**: for write-heavy workloads where batching helps

### `segment_duration`

Time window size for segment files in seconds. Completed windows are sealed into immutable `.seg` files.

- **14,400 (default / 4 hours)**: good compression, reasonable file count
- **3,600 (1 hour)**: more granular retention, smaller files
- **86,400 (1 day)**: maximum compression, fewer files

### `compression_level`

Zstd container compression level (1-19). ALP encoding handles the bulk of compression — zstd is a secondary pass that adds marginal gains. Higher levels produce negligibly smaller output at significant CPU cost.

- **2 (default)**: optimal. ALP output compresses equally well at all levels.
- **1**: marginally faster, same ratio
- **9+**: not recommended. Adds CPU cost with <0.2 B/pt improvement.

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
