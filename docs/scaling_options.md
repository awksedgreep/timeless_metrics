# TimelessMetrics Scaling Options

Performance on AWS i8g.24xlarge (96 vCPU ARM, 768 GiB RAM), 200K series:

| Metric | Value |
|--------|-------|
| Sustained writes (256 writers) | 4.39M pts/sec |
| Query rate (24 workers + writes) | 7,853 q/sec |
| Query latency | 3.06ms |
| Storage efficiency | 0.7 bytes/pt |

---

## Architecture Overview

TimelessMetrics uses a **sharded ETS write buffer** architecture: incoming points land in one of N lock-free ETS tables, paired with N SegmentBuilder workers that compress and write segments to disk. No per-series processes exist.

```
write(store, metric, labels, value)
  → SeriesRegistry.get_or_create (persistent_term + ETS lookup)
  → Buffer.write (ETS insert, lock-free)
  → SegmentBuilder accumulates, compresses, writes to disk
```

---

## Write Path Scaling

### How it scales

1. **Sharded ETS buffers**: N independent ETS tables with `write_concurrency: :auto`. Each table has its own lock striping, so more shards = less contention. Default is `schedulers / 2`.

2. **Lock-free series creation**: New series get IDs from `:atomics` and register via `:ets.insert_new` (CAS). No GenServer in the creation hot path.

3. **Parallel compression**: Each shard has its own SegmentBuilder that compresses independently. Compression is offloaded to Tasks to avoid blocking ingestion.

4. **Disk writes**: each shard writes to its own directory. No cross-shard contention on disk I/O.

### Scaling strategies

| Strategy | When needed | Expected gain |
|----------|-------------|---------------|
| Increase `buffer_shards` | Write contention on high-core machines | More parallel write paths |
| Increase `flush_threshold` | Reduce flush overhead | Larger batches to SegmentBuilder |
| Pre-resolved writes (`write_resolved`) | Hot path with known series IDs | Skip registry lookup |
| Concurrent `write_batch` callers | Multiple data sources | Linear scaling with caller count |

### Pre-resolved writes

For hot paths where you write to the same series repeatedly, resolve the series ID once and bypass the registry on subsequent writes:

```elixir
series_id = TimelessMetrics.resolve_series(:metrics, "cpu_usage", %{"host" => "web-1"})
TimelessMetrics.write_resolved(:metrics, series_id, 73.2, timestamp: ts)
```

---

## Query Path Scaling

### Current approach

Queries read from compressed segments on disk via lock-free file operations. Multi-series queries group by shard and fan out via `Task.async_stream` — each shard's series are queried together to avoid cross-shard file contention.

### Strategies

- **Label filters**: Always filter by labels to reduce fan-out scope
- **Aggregated queries**: Use `query_aggregate_multi` instead of raw queries for dashboards
- **Daily rollups**: Use `query_daily` for long time ranges (reads pre-computed tier data instead of decompressing raw segments)

---

## Memory Scaling

Memory usage is primarily in the ETS buffer shards and SegmentBuilder in-memory segments:

- **ETS buffers**: each shard holds points between flushes. Bounded by `flush_threshold × shard_count`.
- **SegmentBuilder**: accumulates points in memory until the segment window completes. Bounded by `segment_duration` × write rate per shard.
- **Series registry**: persistent_term map + ETS overflow. ~100 bytes per series.

### Estimates

| Series count | Registry memory | Buffer memory (default settings) |
|-------------|----------------|----------------------------------|
| 10K | ~1 MB | ~50 MB |
| 100K | ~10 MB | ~50 MB |
| 500K | ~50 MB | ~50 MB |
| 1M+ | ~100 MB | ~50 MB |

Buffer memory is independent of series count — it depends on write rate and flush settings.

---

## Scale Thresholds

| Series Count | Expected Bottleneck | Mitigation |
|-------------|-------------------|------------|
| 10K | None | Current architecture works well |
| 100K | Series creation time | Already lock-free, scales with cores |
| 500K | Registry persistent_term size | Monitor publish cycle times |
| 1M+ | Single-node limits | Multiple store instances or partitioning |

---

## What NOT to Do

- **Don't add a connection pool for SQLite writes.** SQLite is only used for metadata. Raw data goes to segment files via SegmentBuilder.

- **Don't switch to PostgreSQL/TimescaleDB.** The whole point is embedded, zero-dependency. Network round-trips to an external DB would negate the query latency advantage.

- **Don't pre-optimize for 1M+ series.** The current architecture handles 200K series at 4.39M pts/sec. Optimize when you hit actual bottlenecks, not hypothetical ones.
