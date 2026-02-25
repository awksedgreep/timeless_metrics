# Architecture

This document describes the internal architecture of TimelessMetrics. For the public API, see [API Reference](API.md).

## Supervision tree

```
TimelessMetrics.Supervisor (:metrics_sup)
├── TimelessMetrics.DB (:metrics_db)
│     SQLite database (WAL mode) for series registry, metadata, annotations, alerts
├── Registry (:metrics_actor_registry)
│     Maps {metric_name, labels} → series actor PID
├── DynamicSupervisor (:metrics_actor_sup)
│     Supervises per-series actor processes
├── TimelessMetrics.Actor.SeriesManager (:metrics_actor_manager)
│     Creates/finds series actors, routes writes
├── TimelessMetrics.Actor.Rollup (:metrics_rollup)
│     Periodic daily rollup aggregation
├── TimelessMetrics.Actor.Retention (:metrics_retention)
│     Periodic cleanup of expired raw and rollup data
├── TimelessMetrics.AlertEvaluator (:metrics_alert_evaluator)
│     Periodic alert rule evaluation
├── TimelessMetrics.SelfMonitor (:metrics_self_monitor)
│     Writes internal store metrics (series count, points, storage bytes)
├── DynamicSupervisor (:metrics_scrape_sup)
│     Supervises per-target scrape worker processes
└── TimelessMetrics.Scraper (:metrics_scraper)
      Manages scrape targets, starts/stops workers
```

The supervisor uses `:rest_for_one` strategy, so a crash in the DB or registry restarts everything downstream.

## Per-series actor model

Every unique combination of `{metric_name, labels}` gets its own GenServer process (a "series actor"). This design provides:

- **Write isolation**: writes to different series never contend
- **Natural backpressure**: if a series falls behind, only that series is affected
- **Memory control**: each actor manages its own ring buffer and can be individually garbage collected
- **Crash isolation**: a corrupted series doesn't affect others

Series actors are started on-demand by the `SeriesManager` when a write arrives for a new series, and are registered in an Elixir `Registry` for fast lookup.

## Write path

```
write(store, metric_name, labels, value)
  │
  ▼
Engine.write/5
  │
  ▼
SeriesManager.ensure_series/4
  ├── Registry lookup (fast path: actor already exists)
  └── Start new SeriesServer via DynamicSupervisor (cold path)
  │
  ▼
SeriesServer.write/2
  │
  ▼
Raw buffer (in-process list)
  │  (when buffer reaches block_size or flush_interval fires)
  ▼
Block compression (Gorilla delta-of-delta + XOR encoding, then Zstd)
  │
  ▼
BlockStore: append compressed block to .dat file on disk
```

**Batch writes** (`write_batch/2`) group entries by series key and send one message per series, avoiding per-point message overhead.

## Read path

```
query(store, metric_name, labels, opts)
  │
  ▼
Engine dispatches to the series actor
  │
  ▼
SeriesServer reads from:
  1. Compressed blocks (decompress on the fly)
  2. Current raw buffer (not yet compressed)
  │
  ▼
Merge + filter by time range [from, to]
  │
  ▼
Return [{timestamp, value}, ...]
```

**Multi-series queries** (`query_multi`, `query_aggregate_multi`, etc.) fan out to all matching series actors via `Task.async_stream` for parallel reads, then merge results.

## Storage format

### Compressed blocks

Each series stores data in a ring buffer of compressed blocks:

1. **Gorilla encoding**: timestamps use delta-of-delta encoding; values use XOR encoding (same as Facebook's Gorilla paper). This exploits the regularity of time series data.
2. **Zstd compression**: the Gorilla-encoded binary is further compressed with Zstd for additional ~2-3x reduction.
3. **Block files**: compressed blocks are persisted to `.dat` files in the data directory, one file per series.

Typical compression ratio is ~11.5x (0.67 bytes per point).

### Ring buffer

Each series maintains up to `max_blocks` compressed blocks. When the limit is reached, the oldest block is evicted. This bounds memory usage per series to `max_blocks * block_size` points worth of compressed data.

### SQLite index

SQLite (in WAL mode with mmap) stores:

- **Series registry**: maps `{metric_name, labels_hash}` to series IDs
- **Metric metadata**: type, unit, description
- **Annotations**: event markers with timestamps and tags
- **Alert rules and history**: threshold conditions, evaluation state
- **Scrape targets and health**: Prometheus target configuration
- **Daily rollup data**: pre-aggregated daily statistics

SQLite is not used for raw time series data -- that goes directly to compressed block files for maximum throughput.

## Block merge compaction

Each series actor accumulates many small compressed blocks over time (one per `block_size` points or stale-buffer flush). The merge compaction pass consolidates adjacent old blocks into fewer, larger blocks. Larger blocks achieve better compression ratios (bigger Gorilla + zstd dictionary window) and reduce per-block decompression overhead during large-range queries.

Merge runs inside each `SeriesServer` process -- there is no centralized compactor. It is triggered by a periodic timer (default every 5 minutes) and can also be forced across all series via `TimelessMetrics.merge_now(store)`.

```
Per-series compressed block queue: [b1] [b2] [b3] [b4] [b5] [b6] ... [bN]
  │
  ▼
Age check: only blocks with end_ts older than merge_block_min_age_seconds
  │
  ▼
Count check: eligible blocks >= merge_block_min_count
  │
  ▼
Group into batches of ≈ merge_block_max_points
  │
  ▼
For each batch:
  Decompress blocks → concatenate points → sort by timestamp → recompress
  │
  ▼
Replace batch entries in queue with single merged block
```

| Configuration | Default | Description |
|---------------|---------|-------------|
| `merge_block_min_count` | 4 | Minimum eligible blocks before merge triggers |
| `merge_block_max_points` | 10,000 | Target points per merged block |
| `merge_block_min_age_seconds` | 300 | Only merge blocks older than this (avoids churning recent data) |
| `merge_interval` | 300,000 | Merge check timer interval in ms |

The age threshold ensures recent blocks (still likely to be queried individually) are left alone, while older blocks that are typically scanned in bulk are consolidated for efficiency.

## Rollup pipeline

The `Rollup` actor runs periodically (default: every 5 minutes) and computes daily aggregates:

```
Raw points for each series
  │
  ▼
Group by day (UTC midnight boundaries)
  │
  ▼
Compute aggregates: avg, min, max, sum, count, last
  │
  ▼
Store in SQLite daily_rollups table
```

Daily rollups enable efficient long-range queries. A 90-day query that would scan millions of raw points instead reads ~90 pre-computed rows.

Query daily rollups via:
- `TimelessMetrics.query_daily(store, metric, labels, from, to)`

## Retention

The `Retention` actor runs periodically (default: every hour) and enforces two retention policies:

| Tier | Default retention | What's deleted |
|------|-------------------|----------------|
| Raw | 7 days | Compressed blocks older than the cutoff |
| Daily | 365 days | Daily rollup rows older than the cutoff |

Configure via `raw_retention_seconds` and `daily_retention_seconds` supervisor options.

## Further reading

- [Configuration Reference](configuration.md) -- all supervisor options and tuning guidance
- [API Reference](API.md) -- complete Elixir and HTTP API
- [Operations](operations.md) -- monitoring, backup, troubleshooting
- Architecture livebook at `livebook/architecture.livemd` for interactive diagrams
