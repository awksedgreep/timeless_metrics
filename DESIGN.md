# Timeless - Embedded Time Series Storage for Elixir

## Overview

Timeless is an embedded time series database library for Elixir applications.
It combines Gorilla compression (via `gorilla_stream`) with SQLite/libsql for
storage, providing automatic rollups, configurable retention, and microsecond
query latency with zero external dependencies.

**Design principles:**
- Embedded library, not a service — `use Timeless` and go
- Zero network hops — ETS write buffer, local SQLite storage
- Declarative configuration — define tiers once, the engine handles the rest
- Compression first — gorilla + zstd achieves ~2 bytes/sample
- Horizontally scalable — partition via Erlang clustering when ready

---

## Architecture

```
                        ┌─────────────────────────────────────────────────┐
  write_metric()  ────► │  ETS Write Buffer (per metric namespace)        │
                        │  {device_id, metric_id, timestamp, value}       │
                        └──────────────────┬──────────────────────────────┘
                                           │
                               flush (interval or threshold)
                                           │
                                           ▼
                        ┌──────────────────────────────────────────────────┐
                        │  Segment Builder (GenServer per shard)           │
                        │  - Groups points by {device_id, metric_id}      │
                        │  - Gorilla encodes + zstd compresses            │
                        │  - Writes segment blob to SQLite                │
                        └──────────────────┬───────────────────────────────┘
                                           │
                                           ▼
                        ┌──────────────────────────────────────────────────┐
                        │  SQLite Storage                                  │
                        │                                                  │
                        │  ┌──────────────┐  ┌──────────────────────────┐  │
                        │  │ raw_segments  │  │ tier_hourly              │  │
                        │  │ (gorilla     │  │ tier_daily               │  │
                        │  │  blobs)      │  │ tier_monthly             │  │
                        │  └──────────────┘  │ (pre-computed aggregates)│  │
                        │                    └──────────────────────────┘  │
                        │                                                  │
                        │  ┌──────────────────────────────────────────┐    │
                        │  │ _watermarks (rollup progress tracking)   │    │
                        │  │ _metadata   (schema, config, stats)      │    │
                        │  └──────────────────────────────────────────┘    │
                        └──────────────────────────────────────────────────┘
                                           │
                              ┌────────────┼────────────┐
                              ▼            ▼            ▼
                        ┌──────────┐ ┌──────────┐ ┌──────────┐
                        │ Rollup   │ │ Retention│ │  Query   │
                        │ Engine   │ │ Enforcer │ │  Router  │
                        └──────────┘ └──────────┘ └──────────┘
```

---

## Public API

### Setup

```elixir
# In your application supervision tree
defmodule MyApp.Application do
  def start(_type, _args) do
    children = [
      {Timeless, name: :metrics, data_dir: "/var/lib/myapp/metrics"}
    ]
    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

### Writing Metrics

```elixir
# Single point
Timeless.write(:metrics, "cpu_usage", %{device_id: 42, host: "router-7"}, 73.2)

# Batch write (preferred for throughput)
Timeless.write_batch(:metrics, [
  {"cpu_usage",    %{device_id: 42}, 73.2},
  {"memory_used",  %{device_id: 42}, 4_812_000},
  {"if_in_octets", %{device_id: 42, interface: "eth0"}, 1_293_847}
])

# Write with explicit timestamp (unix seconds)
Timeless.write(:metrics, "cpu_usage", %{device_id: 42}, 73.2, timestamp: 1_707_300_000)
```

### Querying

```elixir
# Raw points for a series (auto-selects best tier based on time range)
Timeless.query(:metrics, "cpu_usage", %{device_id: 42},
  from: ~U[2025-01-01 00:00:00Z],
  to:   ~U[2025-01-02 00:00:00Z]
)
# => {:ok, [{1735689600, 73.2}, {1735689900, 71.8}, ...]}

# Aggregated query
Timeless.query(:metrics, "cpu_usage", %{device_id: 42},
  from: ~U[2025-01-01 00:00:00Z],
  to:   ~U[2025-01-07 00:00:00Z],
  aggregate: :avg,
  bucket: :hour
)
# => {:ok, [%{bucket: 1735689600, avg: 72.4}, ...]}

# Latest value across all devices
Timeless.query(:metrics, "cpu_usage", %{},
  aggregate: :last
)
# => {:ok, [%{device_id: 42, last: 73.2}, %{device_id: 43, last: 68.1}, ...]}

# Multi-aggregate
Timeless.query(:metrics, "cpu_usage", %{device_id: 42},
  from: ~U[2025-01-01 00:00:00Z],
  to:   ~U[2025-01-31 00:00:00Z],
  aggregate: [:avg, :min, :max, :p95],
  bucket: :day
)
```

### Configuration & Introspection

```elixir
# Runtime stats
Timeless.info(:metrics)
# => %{
#      series_count: 100_000,
#      raw_segments: 12_340,
#      storage_bytes: 614_000_000,
#      oldest_raw: ~U[2025-01-20 00:00:00Z],
#      tiers: %{hourly: 30_days, daily: 365_days, monthly: :forever},
#      write_rate: 33_333  # samples/sec (1-minute avg)
#    }

# Force a rollup (normally automatic)
Timeless.rollup(:metrics, :hourly)

# Force retention enforcement
Timeless.enforce_retention(:metrics)
```

---

## Configuration

### Declarative Schema (compile-time)

```elixir
defmodule MyApp.MetricSchema do
  use Timeless.Schema

  # How incoming writes are identified
  series_key [:metric_name, :device_id]

  # Write buffer tuning
  buffer flush_interval: :timer.seconds(5),
         flush_threshold: 10_000,
         shards: System.schedulers_online()

  # Segment settings
  segment duration: :timer.hours(1),    # one blob per series per hour
          compression: :zstd             # gorilla + zstd

  # Rollup tiers (each reads from the tier above)
  tier :hourly,
    resolution: :hour,
    aggregates: [:avg, :min, :max, :count, :sum, :last],
    retention: {30, :days}

  tier :daily,
    resolution: :day,
    aggregates: [:avg, :min, :max, :count, :sum, :last],
    retention: {365, :days}

  tier :monthly,
    resolution: {30, :days},
    aggregates: [:avg, :min, :max, :count, :sum],
    retention: :forever

  # Raw data retention (how long to keep gorilla segments)
  raw_retention {7, :days}

  # Rollup schedule
  rollup_interval :timer.minutes(5)

  # Retention cleanup schedule
  retention_interval :timer.hours(1)
end
```

Then reference it at startup:

```elixir
{Timeless, name: :metrics, schema: MyApp.MetricSchema, data_dir: "/var/lib/myapp/metrics"}
```

### Runtime Configuration (application config)

For simpler setups without a schema module:

```elixir
# config/config.exs
config :timeless, :default,
  data_dir: "/var/lib/myapp/metrics",
  series_key: [:metric_name, :device_id],
  segment_duration: :timer.hours(1),
  compression: :zstd,
  raw_retention: {7, :days},
  tiers: [
    hourly:  [resolution: :hour,      retention: {30, :days}],
    daily:   [resolution: :day,       retention: {365, :days}],
    monthly: [resolution: {30, :days}, retention: :forever]
  ]
```

---

## Storage Layout

### SQLite Schema

```sql
-- Raw gorilla-compressed segments
-- One row = one series (device+metric) for one segment_duration window
CREATE TABLE raw_segments (
  series_id    INTEGER NOT NULL,    -- hash of series key
  start_time   INTEGER NOT NULL,    -- unix epoch seconds
  end_time     INTEGER NOT NULL,
  point_count  INTEGER NOT NULL,
  data         BLOB NOT NULL,       -- gorilla + zstd compressed
  PRIMARY KEY (series_id, start_time)
) WITHOUT ROWID;

-- Series registry: maps series_id to its labels
CREATE TABLE series (
  id           INTEGER PRIMARY KEY,
  metric_name  TEXT NOT NULL,
  labels       TEXT NOT NULL,       -- JSON-encoded sorted label map
  created_at   INTEGER NOT NULL,
  UNIQUE(metric_name, labels)
);

CREATE INDEX idx_series_metric ON series(metric_name);

-- Rollup tier tables (one per configured tier)
-- Same structure for hourly, daily, monthly
CREATE TABLE tier_hourly (
  series_id   INTEGER NOT NULL,
  bucket      INTEGER NOT NULL,     -- unix epoch of bucket start
  avg         REAL,
  min         REAL,
  max         REAL,
  count       INTEGER,
  sum         REAL,
  last        REAL,
  PRIMARY KEY (series_id, bucket)
) WITHOUT ROWID;

CREATE TABLE tier_daily   ( /* same as tier_hourly */ );
CREATE TABLE tier_monthly ( /* same as tier_hourly */ );

-- Rollup watermarks: tracks what's been processed per tier
CREATE TABLE _watermarks (
  tier         TEXT PRIMARY KEY,
  last_bucket  INTEGER NOT NULL     -- last fully-rolled-up bucket boundary
) WITHOUT ROWID;

-- Store metadata: schema version, config snapshot, stats
CREATE TABLE _metadata (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID;
```

**Why `WITHOUT ROWID`:** These are all accessed by primary key range scans.
SQLite stores WITHOUT ROWID tables as clustered B-trees on the primary key,
making `(series_id, time_range)` scans sequential reads. This is the single
biggest performance win for time series queries in SQLite.

### Series ID

Series are identified by a hash of their sorted label set:

```elixir
defp series_id(metric_name, labels) do
  labels
  |> Enum.sort()
  |> then(&{metric_name, &1})
  |> :erlang.phash2(2_147_483_647)  # 31-bit hash, fits INTEGER
end
```

The `series` table provides the reverse lookup (id -> labels) for query results.
Hash collisions are handled by checking the `UNIQUE(metric_name, labels)` constraint
on insert — if it fires, we look up the existing id.

---

## Internal Components

### 1. Write Buffer (`Timeless.Buffer`)

- **Implementation:** Sharded ETS tables, one per scheduler
- **Sharding:** `series_id |> rem(shard_count)` routes writes to the correct shard
- **Flush trigger:** Whichever comes first — interval timer or point threshold
- **Flush operation:** Drain the ETS table, group by series_id, hand off to SegmentBuilder
- **Backpressure:** If the SegmentBuilder mailbox exceeds a high-water mark,
  `write/4` returns `{:error, :backpressure}` so callers can buffer or drop

```elixir
defmodule Timeless.Buffer do
  use GenServer

  # Each shard owns one ETS table
  # Writes are lock-free: :ets.insert with write_concurrency: true
  # Flush drains via :ets.tab2list + :ets.delete_all_objects (atomic swap pattern)

  def write(shard, series_id, timestamp, value) do
    :ets.insert(shard, {series_id, timestamp, value})
  end
end
```

### 2. Segment Builder (`Timeless.SegmentBuilder`)

- **Receives** flushed points grouped by series_id
- **Accumulates** points into per-series accumulators until a segment boundary is crossed
- **Encodes** completed segments: `GorillaStream.compress(points, compression: :zstd)`
- **Writes** the compressed blob to SQLite in a single transaction
- **Registers** new series in the `series` table on first encounter

```elixir
defmodule Timeless.SegmentBuilder do
  use GenServer

  # State holds in-progress segments:
  # %{series_id => %{points: [{ts, val}, ...], start_time: ts, metric_name: ..., labels: ...}}

  # When a segment window closes (e.g., top of the hour):
  # 1. Compress:  {:ok, blob} = GorillaStream.compress(points, compression: :zstd)
  # 2. Write:     INSERT INTO raw_segments VALUES (?, ?, ?, ?, ?)
  # 3. Clear:     Remove from accumulator state
end
```

### 3. Rollup Engine (`Timeless.Rollup`)

- **Schedule:** Runs every `rollup_interval` (default 5 minutes)
- **Incremental:** Reads watermark, processes only new data since last run
- **Cascading:** Each tier reads from the tier above it (daily reads hourly, not raw)
- **Idempotent:** Safe to re-run; uses `INSERT OR REPLACE` for rollup rows
- **Ordered:** Processes tiers bottom-up (hourly first, then daily, then monthly)

```elixir
defmodule Timeless.Rollup do
  use GenServer

  def handle_info(:tick, state) do
    for tier <- [:hourly, :daily, :monthly] do
      watermark = get_watermark(tier)
      source = source_for(tier)  # :hourly reads raw_segments, :daily reads tier_hourly, etc.
      now = current_bucket_boundary(tier)

      # For :hourly reading from raw:
      #   SELECT series_id, data FROM raw_segments
      #   WHERE start_time > ?watermark AND start_time <= ?now
      #
      # Decompress each segment, compute aggregates, write to tier table
      #
      # For :daily reading from :hourly:
      #   SELECT series_id, avg, min, max, count, sum, last FROM tier_hourly
      #   WHERE bucket > ?watermark AND bucket <= ?now
      #   GROUP BY series_id, (bucket / 86400 * 86400)
      #
      # Re-aggregate: weighted avg from (avg * count), min of mins, max of maxes, etc.

      update_watermark(tier, now)
    end

    schedule_next(:tick, state.rollup_interval)
    {:noreply, state}
  end
end
```

**Re-aggregation from rollup tiers** (the key to cascading without raw data):

```elixir
# Given hourly rows being rolled up to daily:
defp aggregate_from_tier(rows) do
  %{
    count: Enum.sum(rows, & &1.count),
    sum:   Enum.sum(rows, & &1.sum),
    min:   Enum.min(rows, & &1.min),
    max:   Enum.max(rows, & &1.max),
    avg:   Enum.sum(rows, &(&1.avg * &1.count)) / Enum.sum(rows, & &1.count),
    last:  rows |> Enum.max_by(& &1.bucket) |> Map.get(:last)
  }
end
```

### 4. Retention Enforcer (`Timeless.Retention`)

- **Schedule:** Runs every `retention_interval` (default 1 hour)
- **Per-tier cleanup:** Deletes rows older than the tier's configured retention
- **Vacuum:** Runs `PRAGMA incremental_vacuum` periodically to reclaim disk space
- **Orphan cleanup:** Removes entries from `series` table when no data remains

```elixir
defmodule Timeless.Retention do
  use GenServer

  def handle_info(:enforce, state) do
    now = System.os_time(:second)

    # Drop expired raw segments
    cutoff_raw = now - retention_seconds(:raw)
    execute("DELETE FROM raw_segments WHERE end_time < ?1", [cutoff_raw])

    # Drop expired rollup rows
    for {tier, retention} <- tier_retentions(), retention != :forever do
      cutoff = now - retention_seconds(retention)
      execute("DELETE FROM tier_#{tier} WHERE bucket < ?1", [cutoff])
    end

    # Periodic vacuum (not every run — maybe every 24h)
    maybe_vacuum(state)

    # Clean orphaned series
    execute("""
      DELETE FROM series WHERE id NOT IN (
        SELECT DISTINCT series_id FROM raw_segments
        UNION SELECT DISTINCT series_id FROM tier_hourly
        UNION SELECT DISTINCT series_id FROM tier_daily
        UNION SELECT DISTINCT series_id FROM tier_monthly
      )
    """)

    schedule_next(:enforce, state.retention_interval)
    {:noreply, state}
  end
end
```

### 5. Query Router (`Timeless.Query`)

The query router automatically selects the best data source based on the
requested time range and resolution:

```elixir
defmodule Timeless.Query do

  def execute(store, metric_name, labels, opts) do
    from = opts[:from]
    to   = opts[:to] || DateTime.utc_now()
    bucket = opts[:bucket]
    aggregate = opts[:aggregate]

    cond do
      # Raw data requested and available
      is_nil(aggregate) and within_retention?(:raw, from) ->
        read_raw_segments(store, metric_name, labels, from, to)

      # Hourly resolution or time range fits hourly tier
      bucket in [:hour, nil] and within_retention?(:hourly, from) ->
        read_tier(store, :hourly, metric_name, labels, from, to, aggregate)

      # Daily
      bucket in [:day, nil] and within_retention?(:daily, from) ->
        read_tier(store, :daily, metric_name, labels, from, to, aggregate)

      # Monthly
      true ->
        read_tier(store, :monthly, metric_name, labels, from, to, aggregate)
    end
  end

  # For queries spanning multiple tiers (e.g., last 60 days with hourly buckets):
  # - Days 1-7:   decompress raw segments, bucket into hours
  # - Days 8-30:  read from tier_hourly directly
  # - Days 31-60: re-expand tier_daily (lose intra-day granularity but data is there)
  defp read_spanning(store, tiers, metric_name, labels, from, to) do
    tiers
    |> Enum.flat_map(fn {tier, tier_from, tier_to} ->
      read_tier(store, tier, metric_name, labels, tier_from, tier_to, nil)
    end)
    |> Enum.sort_by(&elem(&1, 0))
  end
end
```

---

## Supervision Tree

```
Timeless.Supervisor (:metrics)
├── Timeless.DB                    # Exqlite connection pool (SQLite)
├── Timeless.SeriesRegistry        # ETS table mapping series keys to IDs
├── Timeless.Buffer.Supervisor     # DynamicSupervisor for buffer shards
│   ├── Timeless.Buffer.Shard_0
│   ├── Timeless.Buffer.Shard_1
│   ├── ...
│   └── Timeless.Buffer.Shard_N
├── Timeless.SegmentBuilder        # Compresses and writes segments
├── Timeless.Rollup                # Periodic rollup engine
└── Timeless.Retention             # Periodic retention enforcer
```

Each named Timeless instance (`:metrics`, `:logs`, etc.) gets its own
supervision tree, its own SQLite database file, and its own ETS tables.
Multiple instances can coexist in the same application.

---

## SQLite Connection Strategy

SQLite has a single-writer constraint. We handle this with a dedicated writer
and concurrent readers:

```elixir
defmodule Timeless.DB do
  # One write connection (serialized through a GenServer)
  # N read connections (pool, concurrent via WAL mode)

  def init(data_dir) do
    db_path = Path.join(data_dir, "metrics.db")

    # Writer connection
    {:ok, writer} = Exqlite.Sqlite3.open(db_path)
    execute(writer, "PRAGMA journal_mode = WAL")
    execute(writer, "PRAGMA synchronous = NORMAL")
    execute(writer, "PRAGMA cache_size = -64000")        # 64MB cache
    execute(writer, "PRAGMA auto_vacuum = INCREMENTAL")
    execute(writer, "PRAGMA mmap_size = 1073741824")     # 1GB mmap
    execute(writer, "PRAGMA page_size = 8192")           # larger pages for blobs

    # Reader pool: N connections for concurrent queries
    readers = for _ <- 1..System.schedulers_online() do
      {:ok, reader} = Exqlite.Sqlite3.open(db_path, mode: :readonly)
      execute(reader, "PRAGMA mmap_size = 1073741824")
      reader
    end

    {:ok, %{writer: writer, readers: readers}}
  end
end
```

**WAL mode** is essential — it allows concurrent reads while a write is in progress.
This means rollup computation, query serving, and segment writes can all happen
simultaneously without blocking each other.

---

## Compression Pipeline Detail

### Write Path

```
Raw points: [{1735689600, 73.2}, {1735689900, 71.8}, ...]
                    │
                    ▼
        GorillaStream.compress(points, compression: :zstd)
                    │
                    │  1. VictoriaMetrics preprocessing (scale decimals, delta encoding)
                    │  2. Gorilla encoding (delta-of-delta timestamps, XOR float compression)
                    │  3. zstd container compression
                    │
                    ▼
        Compressed blob: <<...>>  (~2 bytes per sample)
                    │
                    ▼
        INSERT INTO raw_segments (series_id, start_time, end_time, point_count, data)
```

### Read Path

```
        SELECT data FROM raw_segments WHERE series_id = ? AND start_time BETWEEN ? AND ?
                    │
                    ▼
        GorillaStream.decompress(blob, compression: :zstd)
                    │
                    │  1. zstd decompression
                    │  2. Gorilla decoding (reconstruct timestamps + values)
                    │  3. Reverse VictoriaMetrics preprocessing
                    │
                    ▼
        [{1735689600, 73.2}, {1735689900, 71.8}, ...]
```

### Expected Compression Ratios

Based on gorilla_stream benchmarks and our DDNet benchmark data:

| Data Pattern          | Gorilla Only | Gorilla + zstd |
|-----------------------|-------------|----------------|
| Smooth metrics (CPU)  | ~1.4 bits/sample | ~1.0 bits/sample |
| Noisy metrics (jitter)| ~6 bits/sample   | ~3 bits/sample   |
| Counters (monotonic)  | ~0.5 bits/sample | ~0.4 bits/sample |
| **Average**           | ~3 bits/sample   | **~2 bytes/sample** |

---

## Horizontal Scaling (Future)

When a single node isn't enough, Timeless partitions across an Erlang cluster
using consistent hashing on `series_id`:

```
                    ┌──────────────────────────────────────┐
  write_metric() ──►  Timeless.Router                   │
                    │  series_id = hash(metric, labels)    │
                    │  node = ConsistentHash.lookup(id)    │
                    └─────┬──────────┬──────────┬──────────┘
                          │          │          │
                          ▼          ▼          ▼
                      Node A     Node B     Node C
                      SQLite     SQLite     SQLite
                   devices 1-10K  10K-20K   20K-30K
```

- **Partition key:** `series_id` (derived from device_id + metric_name)
- **Write routing:** Writes go directly to the owning node (no coordination)
- **Single-device queries:** Hit one node
- **Fleet-wide queries:** Scatter-gather across nodes, merge rollup results
- **Rebalancing:** Lazy — new writes to new assignment, old data expires naturally

This is a future phase. The library should be designed so that the storage and
query interfaces work identically in single-node and clustered mode, with only
the routing layer changing.

---

## Dependencies

```elixir
defp deps do
  [
    {:gorilla_stream, github: "awksedgreep/gorilla_stream"},
    {:exqlite, "~> 0.27"},   # SQLite3 NIF bindings
  ]
end
```

**That's it.** Two runtime dependencies. `gorilla_stream` brings `ezstd` as an
optional dependency (recommended). No Ecto, no Phoenix, no external services.

---

## Project Structure

```
timeless/
├── lib/
│   ├── timeless.ex                  # Public API facade
│   ├── timeless/
│   │   ├── schema.ex                    # DSL macro for compile-time config
│   │   ├── config.ex                    # Runtime config normalization
│   │   ├── supervisor.ex                # Top-level supervisor
│   │   ├── db.ex                        # SQLite connection manager (writer + reader pool)
│   │   ├── db/
│   │   │   └── migrations.ex            # Schema creation and upgrades
│   │   ├── series_registry.ex           # ETS-backed series ID lookup + registration
│   │   ├── buffer.ex                    # Sharded ETS write buffer
│   │   ├── buffer/
│   │   │   └── shard.ex                 # Individual buffer shard GenServer
│   │   ├── segment_builder.ex           # Gorilla compression + SQLite write
│   │   ├── rollup.ex                    # Incremental rollup engine
│   │   ├── retention.ex                 # Retention enforcer + vacuum
│   │   └── query.ex                     # Query router + tier selection
├── test/
│   ├── timeless_test.exs            # Integration tests
│   ├── timeless/
│   │   ├── buffer_test.exs
│   │   ├── segment_builder_test.exs
│   │   ├── rollup_test.exs
│   │   ├── retention_test.exs
│   │   └── query_test.exs
├── bench/
│   ├── write_bench.exs                  # Ingestion throughput benchmarks
│   └── query_bench.exs                  # Query latency benchmarks
├── mix.exs
├── DESIGN.md
└── README.md
```

---

## Build Phases

### Phase 1: Core Storage (MVP)
- [ ] Project scaffold, mix.exs, deps
- [ ] `Timeless.DB` — SQLite connection with WAL mode, migrations
- [ ] `Timeless.SeriesRegistry` — series ID assignment and lookup
- [ ] `Timeless.Buffer` — sharded ETS write buffer with flush
- [ ] `Timeless.SegmentBuilder` — gorilla encode + write to SQLite
- [ ] `Timeless.Query` — read raw segments, decompress, return points
- [ ] `Timeless` — public API facade (`write/4`, `query/4`, `info/1`)
- [ ] Basic tests

**Milestone:** `Timeless.write()` and `Timeless.query()` work end-to-end.

### Phase 2: Rollups & Retention
- [ ] `Timeless.Schema` — declarative tier DSL
- [ ] `Timeless.Rollup` — incremental rollup engine with watermarks
- [ ] `Timeless.Retention` — tier-aware retention + vacuum
- [ ] Query router — automatic tier selection based on time range
- [ ] Cross-tier query stitching

**Milestone:** Automatic rollups running on schedule, old data cleaned up.

### Phase 3: Production Hardening
- [ ] Backpressure on write path
- [ ] Segment builder crash recovery (re-read unflushed ETS on restart)
- [ ] Telemetry integration (`:telemetry.execute` for write/query/rollup events)
- [ ] `Timeless.info/1` with live stats
- [ ] Benchmarks: write throughput, query latency, compression ratios
- [ ] Property-based tests for compression round-trip correctness

**Milestone:** Production-ready for single-node deployment in DDNet.

### Phase 4: Clustering (Future)
- [ ] Consistent hash ring for series routing
- [ ] Write forwarding to owning node
- [ ] Scatter-gather queries
- [ ] Lazy rebalancing on node join/leave
