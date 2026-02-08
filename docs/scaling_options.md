# Timeless Scaling Options

Current performance baseline (22-core laptop, thermal throttling):

| Metric | Value |
|--------|-------|
| Ingest (native) | 225K pts/sec |
| Ingest (VictoriaMetrics, HTTP) | 782K pts/sec |
| Single-series query | 50-110 us |
| Multi-series query (50 hosts) | 2.65 ms |
| Storage efficiency | 0.16-0.2 B/pt |

Target: close the ingestion gap with VictoriaMetrics while maintaining the
embedded single-binary advantage.

---

## Ingestion Pipeline Bottlenecks

Current write path and where time is spent:

```
write_batch()                          SERIAL Enum.each over entries
  -> SeriesRegistry.get_or_create()    ETS lookup (fast) or GenServer (first time)
  -> Buffer.write()                    ETS insert (lock-free, fast)
  -> GenServer.cast(:maybe_flush)      Async, but 1 cast per write
  -> [threshold reached]
  -> Buffer.do_flush()                 tab2list + group_by + cast to builder
  -> SegmentBuilder.ingest()           Single GenServer, all shards funnel here
  -> [segment complete]
  -> write_segments()                  Gorilla compress + SQLite transaction
```

### Bottleneck 1: write_batch is serial (biggest win)

`write_batch` calls `Enum.each` over every entry, doing a series registry
lookup + ETS insert + GenServer cast per point. For 2000 entries per batch
(100 devices x 20 metrics), that's 2000 sequential operations.

**Fix: Pre-group entries by shard, bulk-insert into ETS, one cast per shard.**

```elixir
def write_batch(store, entries) do
  registry = :"#{store}_registry"
  shard_count = buffer_shard_count(store)

  # Resolve all series IDs and group by shard in one pass
  entries
  |> Enum.map(fn
    {metric, labels, value} ->
      sid = SeriesRegistry.get_or_create(registry, metric, labels)
      {sid, System.os_time(:second), value}
    {metric, labels, value, ts} ->
      sid = SeriesRegistry.get_or_create(registry, metric, labels)
      {sid, ts, value}
  end)
  |> Enum.group_by(fn {sid, _, _} -> rem(abs(sid), shard_count) end)
  |> Enum.each(fn {shard_idx, points} ->
    table = :"#{store}_shard_#{shard_idx}_buf"
    # Bulk insert — single ETS call per shard
    rows = Enum.map(points, fn {sid, ts, val} ->
      {{sid, ts, :erlang.unique_integer()}, val}
    end)
    :ets.insert(table, rows)
    GenServer.cast(:"#{store}_shard_#{shard_idx}", :maybe_flush)
  end)
end
```

**Expected gain: 2-3x** — eliminates per-point GenServer.cast overhead and
reduces ETS insert calls from N to shard_count.

### Bottleneck 2: Buffer flush overhead

`do_flush` calls `:ets.tab2list` (copies entire table), filters metadata,
maps to strip unique integers, then `Enum.group_by` to group by series_id.
Three full passes over the data.

**Fix: Use `:ets.take` or `:ets.select_delete` instead of tab2list + delete_all.
Skip the unique integer on read by pattern-matching it away in the select.**

```elixir
defp do_flush(state) do
  # Match all data points, skip metadata
  match_spec = [{{{:"$1", :"$2", :_}, :"$3"}, [{:is_integer, :"$1"}], [{{:"$1", :"$2", :"$3"}}]}]
  points = :ets.select(state.table, match_spec)
  :ets.match_delete(state.table, {{:"$1", :_, :_}, :_})  # Keep metadata keys
  # points is already [{series_id, ts, val}, ...] — one pass to group
  grouped = Enum.group_by(points, &elem(&1, 0), fn {_, ts, val} -> {ts, val} end)
  if grouped != %{} do
    SegmentBuilder.ingest(state.segment_builder, grouped)
  end
end
```

**Expected gain: 20-30%** on flush time — fewer allocations, one pass instead
of three.

### Bottleneck 3: SegmentBuilder is a single GenServer

All N buffer shards cast to one SegmentBuilder. The builder:
1. Reduces all points into its segment map (CPU-bound)
2. Checks for completed segments
3. Compresses and writes completed segments (blocks on SQLite)

At high throughput, the builder's mailbox grows faster than it drains.

**Fix: Shard the SegmentBuilder the same way buffers are sharded.**

```
Buffer[0] -> SegmentBuilder[0] -> SQLite (via shared DB GenServer)
Buffer[1] -> SegmentBuilder[1] ->   "
Buffer[2] -> SegmentBuilder[2] ->   "
...
```

Each buffer shard gets its own builder. Compression runs in parallel across
all builders. SQLite writes still serialize through the DB GenServer, but
the compression bottleneck is eliminated.

**Expected gain: N x** where N = core count for compression. SQLite writes
become the new bottleneck (see #4).

### Bottleneck 4: SQLite single-writer serialization

SQLite only allows one writer at a time. The DB GenServer serializes all
write calls. With sharded SegmentBuilders, multiple builders would queue
behind the writer.

**Fix A: Batch more aggressively.** Instead of writing segments as they
complete, accumulate completed segments and write them all in one large
transaction. One `BEGIN IMMEDIATE` + N inserts + `COMMIT` is much faster
than N separate transactions.

**Fix B: Prepared statement caching.** The current `execute` function
prepares a new statement every call. Cache the INSERT OR REPLACE statement
once and rebind parameters.

```elixir
# In DB GenServer state, cache the prepared statement
{:ok, insert_stmt} = Exqlite.Sqlite3.prepare(writer,
  "INSERT OR REPLACE INTO raw_segments (series_id, start_time, end_time, point_count, data) VALUES (?1, ?2, ?3, ?4, ?5)")

# On each insert: bind + step + reset (no prepare/release overhead)
Exqlite.Sqlite3.bind(insert_stmt, params)
Exqlite.Sqlite3.step(writer, insert_stmt)
Exqlite.Sqlite3.reset(insert_stmt)  # Reuse for next bind
```

**Fix C: Direct connection access for writes.** The DB GenServer handles
reads and writes through `handle_call`. For write transactions, the
SegmentBuilder could hold a direct reference to the writer connection
(as it did before the DB module existed) and bypass the GenServer entirely.
This eliminates one message-pass per write transaction.

**Expected gain: 2-4x** combined — prepared statements alone save ~30% on
SQLite overhead. Batching saves another 2-3x by amortizing transaction cost.

### Bottleneck 5: Per-point GenServer cast in Buffer.write

Every `Buffer.write` call sends `GenServer.cast(shard, :maybe_flush)` to
check if the threshold is reached. At 200K writes/sec with 22 shards,
that's ~9K casts/sec per shard just to increment a counter.

**Fix: Use `:atomics` for the point counter instead of GenServer state.**

```elixir
def write(shard_name, series_id, timestamp, value) do
  table = table_name(shard_name)
  case check_backpressure(shard_name) do
    :ok ->
      :ets.insert(table, {{series_id, timestamp, :erlang.unique_integer()}, value})
      # Atomic increment, no GenServer involved
      count = :atomics.add_get(counter_ref(shard_name), 1, 1)
      if count >= threshold(shard_name) do
        GenServer.cast(shard_name, :flush_now)
        :atomics.put(counter_ref(shard_name), 1, 0)
      end
      :ok
    err -> err
  end
end
```

**Expected gain: 15-20%** — eliminates GenServer message for every write,
keeps only the periodic flush signal.

---

## Projected Gains

Applying these in order of effort/impact:

| Fix | Effort | Expected Gain | Cumulative |
|-----|--------|--------------|------------|
| Bulk write_batch | Small | 2-3x | ~500K pts/sec |
| Atomics counter | Small | 15-20% | ~600K pts/sec |
| Prepared statement cache | Small | 30% SQLite | ~650K pts/sec |
| Buffer flush optimization | Small | 20-30% flush | ~700K pts/sec |
| Shard SegmentBuilder | Medium | Nx compression | ~800K+ pts/sec |
| Direct writer access | Medium | Skip GenServer | ~900K pts/sec |

These are estimates for the same thermal-throttled hardware. On a proper
server (Xeon, no throttling), baseline would be higher and gains would
compound further.

**Conservative target: 600-700K pts/sec** with just the small fixes.
**Aggressive target: 800K-1M pts/sec** with sharded builders.

For reference, VictoriaMetrics achieves 782K pts/sec via HTTP on the same
hardware, so reaching 700K+ with native API would make Timeless faster
than VM for the embedded use case.

---

## Query Scaling Options

### Multi-series query optimization

Currently: `Task.async_stream` runs N parallel SQLite reads. Each read
does a full prepare/bind/step/release cycle.

**Fix: Single SQL query with `IN` clause for batch series lookup.**

```elixir
def raw_multi(db, series_ids, opts) do
  placeholders = Enum.map_join(1..length(series_ids), ",", &"?#{&1}")
  {:ok, rows} = DB.read(db, """
    SELECT series_id, data, start_time, end_time
    FROM raw_segments
    WHERE series_id IN (#{placeholders})
      AND end_time >= ?#{length(series_ids) + 1}
      AND start_time <= ?#{length(series_ids) + 2}
    ORDER BY series_id, start_time
  """, series_ids ++ [from, to])

  # Group and decompress in parallel
  rows
  |> Enum.group_by(&List.first/1)
  |> Task.async_stream(fn {series_id, segs} -> decompress(segs) end)
end
```

**Expected gain: 3-5x** for multi-series — one SQLite query instead of N,
parallel decompression for the CPU-bound part.

### Rollup parallelism

Rollup currently processes tiers sequentially. Tiers 2+ (daily, monthly)
are independent once their source tier is updated.

**Fix: `Task.async` for independent tiers.**

Gain is modest (~20% faster rollups) since most time is in the hourly tier.

---

## Scale Thresholds

| Series Count | Expected Bottleneck | Mitigation |
|-------------|-------------------|------------|
| 10K (current) | None | Current architecture works |
| 50K | write_batch serialization | Bulk write_batch fix |
| 200K | SegmentBuilder mailbox | Shard SegmentBuilder |
| 1M | SQLite writer queue | Prepared statements + batching |
| 2M+ | SQLite file size (~6 GB) | Consider sharded DBs by metric |
| 10M+ | ETS memory for series registry | Disk-backed registry |

---

## What NOT to Do

- **Don't add a connection pool for SQLite writes.** SQLite fundamentally
  only supports one writer. A pool would just serialize with extra overhead.

- **Don't switch to PostgreSQL/TimescaleDB.** The whole point is embedded,
  zero-dependency. Network round-trips to an external DB would negate the
  query latency advantage.

- **Don't pre-optimize for 2M series.** The current architecture handles
  10K-50K well. Optimize when you hit actual bottlenecks, not hypothetical
  ones. The fixes above are ordered by when you'll need them.

- **Don't add GenStage/Flow.** The current push-based architecture
  (ETS -> GenServer cast) is simpler and faster than a pull-based pipeline
  for this workload. GenStage adds latency for demand coordination.
