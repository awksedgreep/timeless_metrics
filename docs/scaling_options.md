# TimelessMetrics Scaling Options

Current performance baseline (28-core laptop, DDR5-5600):

| Metric | Value |
|--------|-------|
| Sequential writes (10K series) | 746K pts/sec |
| Concurrent saturation (28 cores) | 5.7M pts/sec |
| Single-series query | ~441us |
| Fan-out 100 series | ~1ms |
| Fan-out 1K series | ~10ms |
| Latest value | ~171us |
| Storage efficiency | ~0.67 B/pt |

---

## Architecture Overview

TimelessMetrics uses a **per-series actor model**: every unique `{metric_name, labels}` combination gets its own GenServer process (`SeriesServer`). Writes are non-blocking `GenServer.cast` calls, reads are `GenServer.call` with parallel fan-out via `Task.async_stream`.

```
write(store, metric, labels, value)
  → SeriesManager.get_or_start (ETS + Registry lookup)
  → GenServer.cast(pid, {:write, ts, val})
  → SeriesServer accumulates in raw_buffer
  → On block_size reached: Gorilla + zstd compress → ring buffer
  → Periodic flush: BlockStore.write → .dat file on disk
```

---

## Write Path Scaling

### Current bottlenecks

1. **Series resolution**: Each write resolves `{metric, labels}` to a PID via ETS index + Registry lookup. This is fast (microseconds) but sequential per write call.

2. **Batch write grouping**: `write_batch/2` groups entries by series and sends one cast per series. Grouping cost is O(N) but sequential.

3. **Per-series compression**: Gorilla + zstd compression happens inside each SeriesServer when `block_size` is reached. This is naturally parallel across series.

4. **Disk flush**: `BlockStore.write` does atomic tmp + rename per series. Flushes are staggered by the 60s timer per actor.

### Scaling strategies

| Strategy | When needed | Expected gain |
|----------|-------------|---------------|
| Increase `block_size` | Compression ratio matters more than flush latency | Better compression (larger Gorilla windows) |
| Pre-resolved writes (`write_resolved`) | Hot path with known series IDs | Skip ETS + Registry lookup |
| Concurrent `write_batch` callers | Multiple data sources | Linear scaling with caller count |
| Reduce `flush_interval` | Lower data loss tolerance | More frequent disk writes |

### Pre-resolved writes

For hot paths where you write to the same series repeatedly, resolve the series ID once and bypass the registry on subsequent writes:

```elixir
series_id = TimelessMetrics.resolve_series(:metrics, "cpu_usage", %{"host" => "web-1"})
TimelessMetrics.write_resolved(:metrics, series_id, 73.2)
```

---

## Query Path Scaling

### Current approach

Multi-series queries fan out `GenServer.call` requests to all matching series via `Task.async_stream` with `max_concurrency: System.schedulers_online()`.

### Scaling characteristics

| Series count | Fan-out latency | Bottleneck |
|-------------|----------------|------------|
| 1 | ~441us | None |
| 100 | ~1ms | None |
| 1K | ~10ms | Scheduler saturation |
| 10K | ~1.8s | Process scheduling + decompression |

### Strategies

- **Label filters**: Always filter by labels to reduce fan-out scope
- **Aggregated queries**: Use `query_aggregate_multi` instead of raw queries for dashboards
- **Daily rollups**: Use `query_daily` for long time ranges (reads ~N rows from SQLite instead of decompressing blocks)
- **Merge compaction**: Consolidate small blocks into larger ones for faster large-range queries (`TimelessMetrics.merge_now/1`)

---

## Memory Scaling

Each series actor consumes memory for:
- Process overhead: ~2KB base
- Raw buffer: up to `block_size` points (16 bytes each for numeric)
- Compressed block ring: up to `max_blocks` compressed blocks
- ETS index entry: ~100 bytes

### Estimates

| Series count | Base memory | With 100 blocks × 1K points |
|-------------|------------|---------------------------|
| 1K | ~20 MB | ~200 MB |
| 10K | ~200 MB | ~2 GB |
| 100K | ~2 GB | ~20 GB |

Reduce `max_blocks` or `block_size` to bound per-series memory.

---

## Scale Thresholds

| Series Count | Expected Bottleneck | Mitigation |
|-------------|-------------------|------------|
| 10K | None | Current architecture works well |
| 50K | Memory (per-series actors) | Reduce `max_blocks`, increase `block_size` |
| 100K | BEAM process count | Monitor scheduler run queues |
| 500K | ETS index + process memory | Consider partitioning by metric namespace |
| 1M+ | Single-node limits | Multiple store instances or clustering |

---

## What NOT to Do

- **Don't add a connection pool for SQLite writes.** SQLite fundamentally only supports one writer. SQLite is only used for metadata — raw data goes to per-series `.dat` files.

- **Don't switch to PostgreSQL/TimescaleDB.** The whole point is embedded, zero-dependency. Network round-trips to an external DB would negate the query latency advantage.

- **Don't pre-optimize for 500K+ series.** The current architecture handles 10K-100K well. Optimize when you hit actual bottlenecks, not hypothetical ones.

- **Don't add GenStage/Flow.** The current cast-based write path with per-series actors is simpler and faster than a pull-based pipeline for this workload.
