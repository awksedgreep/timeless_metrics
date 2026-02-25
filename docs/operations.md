# Operations

This guide covers monitoring, backup, maintenance, and troubleshooting for TimelessMetrics.

## Store info

### Elixir API

```elixir
info = TimelessMetrics.info(:metrics)
```

Returns a map with:

| Field | Description |
|-------|-------------|
| `series_count` | Number of active metric series |
| `total_points` | Total stored data points across all series |
| `storage_bytes` | Total storage size on disk |
| `raw_buffer_points` | Points currently in the raw buffer (not yet compressed) |
| `bytes_per_point` | Average bytes per point (compression efficiency) |

### HTTP API

```bash
curl http://localhost:8428/health
```

Response:

```json
{
  "status": "ok",
  "series": 150,
  "points": 1250000,
  "storage_bytes": 24000000,
  "buffer_points": 320,
  "bytes_per_point": 0.67
}
```

The `/health` endpoint is always accessible without authentication, making it suitable for load balancer health checks.

## Flushing

Force flush all buffered data to compressed blocks on disk:

```elixir
TimelessMetrics.flush(:metrics)
```

```bash
# There is no dedicated HTTP endpoint for flush, but you can trigger it
# before a backup to ensure all data is persisted.
```

Use this before backups or graceful shutdowns to ensure no data is lost in the buffer.

## Backup

### Elixir API

```elixir
{:ok, result} = TimelessMetrics.backup(:metrics, "/tmp/metrics_backup")
```

Returns:

```elixir
{:ok, %{
  path: "/tmp/metrics_backup",
  files: ["metrics.db", "series_1.dat", "series_2.dat", ...],
  total_bytes: 24000000
}}
```

### HTTP API

```bash
# Backup to a server-side directory
curl -X POST http://localhost:8428/api/v1/backup \
  -H 'Content-Type: application/json' \
  -d '{"path": "/tmp/metrics_backup"}'

# Backup to default location (data_dir/backups/timestamp)
curl -X POST http://localhost:8428/api/v1/backup
```

Response:

```json
{
  "status": "ok",
  "path": "/tmp/metrics_backup",
  "files": ["metrics.db", "series_1.dat"],
  "total_bytes": 24000000
}
```

### Backup procedure

1. Flush buffered data: `TimelessMetrics.flush(:metrics)`
2. Create backup: `TimelessMetrics.backup(:metrics, target_dir)`
3. The backup creates a consistent snapshot of all SQLite databases and data files
4. Copy the backup directory to offsite storage

### Restore procedure

1. Stop the TimelessMetrics instance
2. Replace the data directory contents with the backup files
3. Start the instance -- it will load from the restored data

## Merge compaction

Each series actor periodically consolidates multiple small compressed blocks into fewer, larger blocks for better compression and faster large-range queries. This runs automatically (default: every 5 minutes) but can be triggered manually.

### Manual trigger

```elixir
TimelessMetrics.merge_now(:metrics)
# => :ok (if any series merged blocks) or :noop (if no merge was needed)
```

### Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `merge_block_min_count` | 4 | Min eligible blocks before merge triggers |
| `merge_block_max_points` | 10,000 | Target points per merged block |
| `merge_block_min_age_seconds` | 300 | Only merge blocks older than this |
| `merge_interval` | 300,000 (5 min) | Merge check interval |

See [Configuration Reference](configuration.md) for tuning guidance.

## Daily rollups

Rollups compute daily aggregates (avg, min, max, sum, count, last) for each series. They run automatically on a configurable interval (default: every 5 minutes).

### Manual trigger

```elixir
TimelessMetrics.rollup(:metrics)
```

### Query rollup data

```elixir
{:ok, rollups} = TimelessMetrics.query_daily(:metrics, "cpu_usage", %{"host" => "web-1"},
  System.os_time(:second) - 30 * 86_400,
  System.os_time(:second))
```

Returns:

```elixir
{:ok, [
  %{bucket: 1700006400, avg: 73.5, min: 45.2, max: 98.7, count: 288, sum: 21168.0, last: 72.1},
  %{bucket: 1700092800, avg: 71.2, min: 42.8, max: 95.3, count: 288, sum: 20505.6, last: 70.8}
]}
```

## Retention enforcement

Retention runs automatically (default: every hour) and deletes data older than the configured thresholds.

### Manual trigger

```elixir
TimelessMetrics.enforce_retention(:metrics)
```

### Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `raw_retention_seconds` | 604,800 (7 days) | How long to keep raw point data |
| `daily_retention_seconds` | 31,536,000 (365 days) | How long to keep daily rollup data |

See [Configuration Reference](configuration.md) for all options.

## Self-monitoring

### Prometheus metrics endpoint

`GET /metrics` exposes metrics in Prometheus text exposition format:

```bash
curl http://localhost:8428/metrics
```

```
# HELP vm_memory_total_bytes Total BEAM memory usage in bytes.
# TYPE vm_memory_total_bytes gauge
vm_memory_total_bytes 123456789
# HELP vm_memory_processes_bytes BEAM process memory in bytes.
# TYPE vm_memory_processes_bytes gauge
vm_memory_processes_bytes 45678901
# HELP vm_memory_ets_bytes BEAM ETS table memory in bytes.
# TYPE vm_memory_ets_bytes gauge
vm_memory_ets_bytes 12345678
# HELP vm_memory_binary_bytes BEAM binary memory in bytes.
# TYPE vm_memory_binary_bytes gauge
vm_memory_binary_bytes 9876543
# HELP vm_memory_atom_bytes BEAM atom memory in bytes.
# TYPE vm_memory_atom_bytes gauge
vm_memory_atom_bytes 654321
# HELP vm_memory_system_bytes BEAM system memory in bytes.
# TYPE vm_memory_system_bytes gauge
vm_memory_system_bytes 78901234
# HELP vm_process_count Number of BEAM processes.
# TYPE vm_process_count gauge
vm_process_count 342
# HELP vm_port_count Number of BEAM ports.
# TYPE vm_port_count gauge
vm_port_count 12
# HELP vm_run_queue_length Total BEAM scheduler run queue length.
# TYPE vm_run_queue_length gauge
vm_run_queue_length 0
# HELP timeless_series_count Number of active metric series.
# TYPE timeless_series_count gauge
timeless_series_count 150
# HELP timeless_total_points Total stored data points.
# TYPE timeless_total_points gauge
timeless_total_points 1250000
# HELP timeless_storage_bytes Storage size in bytes.
# TYPE timeless_storage_bytes gauge
timeless_storage_bytes 24000000
# HELP timeless_buffer_points Number of points in raw buffer.
# TYPE timeless_buffer_points gauge
timeless_buffer_points 320
```

### Monitoring recommendations

Key metrics to watch:

| Metric | Alert threshold | Description |
|--------|-----------------|-------------|
| `timeless_series_count` | Growing unexpectedly | Cardinality explosion (too many label combinations) |
| `timeless_buffer_points` | Consistently high | Flush may be falling behind |
| `vm_memory_total_bytes` | System-dependent | BEAM memory usage |
| `vm_process_count` | > series_count + baseline | Each series is a process |
| `vm_run_queue_length` | Sustained > 0 | Schedulers overloaded |
| `timeless_storage_bytes` | Disk capacity | Storage growth |

### Scrape health

If scraping is enabled, monitor target health:

```bash
curl http://localhost:8428/api/v1/scrape_targets
```

Check the `health` field for each target: `"up"`, `"down"`, or `"unknown"`.

Self-monitoring metrics written per scrape target:

| Metric | Labels | Description |
|--------|--------|-------------|
| `up` | `job`, `instance` | 1 if last scrape succeeded, 0 if failed |
| `scrape_duration_seconds` | `job`, `instance` | Last scrape duration |
| `scrape_samples_scraped` | `job`, `instance` | Samples from last scrape |

## Troubleshooting

### High memory usage

- Check `timeless_series_count` -- high cardinality (many unique label combinations) creates many processes
- Reduce `max_blocks` to limit per-series memory
- Reduce `block_size` to flush more frequently
- Disable `self_monitor: false` if the store is monitoring itself recursively

### Slow queries

- Multi-series queries fan out to all matching series actors. Use label filters to narrow the query.
- Use aggregated queries (`query_aggregate_multi`) instead of raw queries for dashboards
- For long time ranges, use daily rollups (`query_daily`) instead of raw data
- Trigger merge compaction to consolidate small blocks: `TimelessMetrics.merge_now(:metrics)`
- Check `vm_run_queue_length` -- sustained values > 0 indicate CPU saturation

### Disk space growing

- Check retention settings: `raw_retention_seconds` and `daily_retention_seconds`
- Run `TimelessMetrics.enforce_retention(:metrics)` manually to trigger cleanup
- Monitor `timeless_storage_bytes` over time
- High cardinality (many series) multiplies storage linearly

### Scrape failures

- Check target health via `GET /api/v1/scrape_targets`
- The `last_error` field shows the failure reason
- Common causes: target down, DNS resolution failure, timeout, wrong port
- Increase `scrape_timeout` for slow targets

### Data appears missing

- Check that data was actually written: `TimelessMetrics.list_metrics(:metrics)`
- Verify the time range: timestamps are unix seconds, not milliseconds
- Check label filters: exact match is required unless using regex filters
- Flush the buffer: `TimelessMetrics.flush(:metrics)` to ensure buffered data is queryable

### OpenAPI documentation

Interactive API documentation is available at `http://localhost:8428/api/docs`, served via Scalar UI. The OpenAPI spec is at `/api/openapi.json`.
