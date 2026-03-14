# Architecture

This document describes the internal architecture of TimelessMetrics. For the public API, see [API Reference](API.md).

## Supervision tree

```
TimelessMetrics.Supervisor (:metrics_sup)
├── TimelessMetrics.DB (:metrics_db)
│     SQLite database (WAL mode) for series registry, metadata, annotations, alerts
├── TimelessMetrics.SeriesRegistry (:metrics_registry)
│     Series ID lookup (persistent_term + ETS overflow + SQLite backing)
├── TimelessMetrics.DictTrainer (:metrics_dict_trainer)
│     Trains zstd dictionaries for improved compression
├── TimelessMetrics.SegmentBuilder (:metrics_builder_0..N)
│     Per-shard compression worker (accumulates → gorilla + zstd → disk)
├── TimelessMetrics.Buffer (:metrics_shard_0..N)
│     Per-shard ETS write buffer (lock-free inserts)
├── TimelessMetrics.Rollup (:metrics_rollup)
│     Periodic rollup aggregation (hourly, daily tiers)
├── TimelessMetrics.Retention (:metrics_retention)
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

Buffer and SegmentBuilder are paired — each shard has one of each. The default shard count is `max(schedulers_online / 2, 2)`, so a 96-core machine gets 48 shards.

## Sharded write architecture

Every write is routed to one of N ETS buffer shards by series ID. This distributes write contention across independent tables, each with its own lock striping via `write_concurrency: :auto`.

There are no per-series processes. The hot write path is entirely lock-free ETS operations.

## Write path

```
write(store, metric_name, labels, value)
  │
  ▼
SeriesRegistry.get_or_create/3
  ├── persistent_term Map lookup (fast path: series exists)
  ├── ETS overflow lookup (warm path: recently created)
  └── atomics counter + :ets.insert_new (cold path: new series, lock-free)
  │
  ▼
series_id → shard routing: rem(abs(series_id), shard_count)
  │
  ▼
Buffer.write/4 → :ets.insert(shard_table, {{series_id, ts, seq}, value})
  │  (when buffer reaches flush_threshold or flush_interval fires)
  ▼
SegmentBuilder.ingest/2 (GenServer cast — non-blocking)
  │  (accumulates in memory, grouped by {series_id, time_window})
  │  (periodic flush_pending compresses dirty segments → WAL)
  │  (periodic check_segments seals completed windows → .seg files)
  ▼
ShardStore: gorilla + zstd compressed segments on disk
```

**Batch writes** (`write_batch/2`) resolve all series IDs, group by shard, and call `Buffer.write_bulk/2` per shard — one ETS insert per shard regardless of batch size.

**Series creation** is lock-free: new series get IDs from an `:atomics` counter and register via `:ets.insert_new` (atomic CAS). SQLite metadata writes are batched asynchronously on a periodic timer.

## Read path

```
query(store, metric_name, labels, opts)
  │
  ▼
SeriesRegistry resolves series_id
  │
  ▼
Query.raw/3 → routes to the series' shard's SegmentBuilder
  │
  ├── ShardStore.read_segments(series_id, from, to)
  │     Reads compressed .seg files + WAL
  │     Lock-free file I/O, no GenServer involved
  │
  ▼
Decompress segments (gorilla + zstd)
  │
  ▼
Filter by time range, merge, return [{timestamp, value}, ...]
```

**Single-series queries** read from the SegmentBuilder's shard storage via lock-free file operations. No process mailbox is involved, so queries don't compete with writes.

**Multi-series queries** (`query_multi`, `query_aggregate_multi`, etc.) group matching series by shard and fan out via `Task.async_stream`. Each shard's series are queried together, eliminating cross-shard file contention.

## Series Registry

The SeriesRegistry uses a three-tier lookup for maximum throughput:

| Tier | Mechanism | Latency | When |
|------|-----------|---------|------|
| Published | `persistent_term` Map | Zero-copy, sub-microsecond | Steady state (after publish timer) |
| Overflow | ETS `:set` with `write_concurrency: :auto` | Lock-free read | Recently created series |
| Creation | `:atomics.add_get` + `:ets.insert_new` | Lock-free CAS | First write to a new series |

Every 5 seconds, the overflow table is merged into the persistent_term map and cleared. In steady state, 100% of lookups hit the persistent_term fast path.

SQLite writes are batched asynchronously — they never block the write path.

## Storage format

### Segments

Data is stored in time-windowed segments per shard:

```
shard_N/
  raw/
    1706000000.seg    # immutable segment file per window
    current.wal       # pending data not yet sealed
  tier_hourly/
    chunks.dat        # append-only tier data
    index.ets
  tier_daily/
    chunks.dat
    index.ets
```

Each segment file contains gorilla-compressed data for multiple series within a time window (default: 4 hours). The compression pipeline:

1. **Gorilla encoding**: timestamps use delta-of-delta; values use XOR encoding (Facebook's Gorilla paper)
2. **Zstd compression**: the gorilla-encoded binary is further compressed with zstd
3. **Optional dictionary compression**: trained zstd dictionaries for additional gains on small segments

Text series use an RLE + zstd codec instead of gorilla encoding, identified by a `0xFE` byte prefix.

Measured compression: **0.7 bytes per point** on production-like workloads (739 MB for 1.13 billion points).

### WAL and sealing

The SegmentBuilder accumulates points in memory. Periodically:

- **flush_pending** (every 60s): compresses only dirty segments (those with new data since last flush) and writes to WAL, making data queryable
- **check_segments** (every 10s): seals completed time windows into immutable `.seg` files

Compression is offloaded to Tasks to avoid blocking the SegmentBuilder's ingest path.

### SQLite

SQLite (WAL mode with mmap) stores metadata only:

- **Series registry**: maps `{metric_name, labels}` to series IDs
- **Metric metadata**: type, unit, description
- **Annotations**: event markers with timestamps and tags
- **Alert rules and history**: threshold conditions, evaluation state
- **Scrape targets and health**: Prometheus target configuration

SQLite is not used for raw time series data — that goes directly to compressed segment files for maximum throughput.

## Rollup pipeline

The `Rollup` process runs periodically (default: every 5 minutes) and computes tiered aggregates per shard:

```
Raw segments per shard
  │
  ▼
Read segments since last watermark
  │
  ▼
Group by day (UTC midnight boundaries)
  │
  ▼
Compute aggregates: avg, min, max, sum, count, last
  │
  ▼
Write tier chunks to ShardStore (binary files, not SQLite)
```

Rollup data is stored in the same shard directory as raw segments, enabling parallel per-shard reads for long-range queries.

## Retention

The `Retention` process runs periodically (default: every hour) and enforces retention policies per shard:

| Tier | Default retention | What's deleted |
|------|-------------------|----------------|
| Raw | 7 days | Segment files older than the cutoff |
| Daily | 365 days | Tier chunk data older than the cutoff |

Configure via `raw_retention_seconds` and `daily_retention_seconds` supervisor options.

## Further reading

- [Configuration Reference](configuration.md) — all supervisor options and tuning guidance
- [API Reference](API.md) — complete Elixir and HTTP API
- [Operations](operations.md) — monitoring, backup, troubleshooting
- [Benchmark Comparison](../notes/blog_benchmark_comparison.md) — Timeless vs VictoriaMetrics on 96-core ARM
