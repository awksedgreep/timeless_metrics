# TimelessMetrics Scaling Notes

This guide focuses on the current rust-default engine. Older ETS shard and SegmentBuilder tuning advice is intentionally omitted here because it no longer describes the default deployment path.

## Architecture overview

The rust engine keeps the hot path inside a single native resource:

```text
write(store, metric, labels, value)
  -> resolve series id in rust
  -> append point to an in-memory partition buffer
  -> flush compressed chunks to disk
```

Supporting features such as alerts, annotations, scrape targets, and rollup metadata still use the Elixir-side SQLite admin database, but raw point ingestion and querying go through the rust engine.

## What scales well

### Batch writes

If you control the caller, `write_batch/2` is the first scaling lever to use.

- It amortizes Elixir-to-NIF overhead.
- It reduces repeated label resolution work.
- It produces the most representative ingest numbers for sustained workloads.

In the current benchmark set, batch ingest is materially stronger than point-at-a-time writes.

### Warm steady-state ingest

TimelessMetrics performs best once the active series set already exists and writes are flowing into established series.

- Cold population includes series creation and metadata persistence.
- Warm ingest reflects ongoing workload throughput.

When benchmarking or capacity planning, separate those two phases. Mixing them leads to misleading conclusions.

### Query fan-out by label filter

Multi-series queries scale best when label filters narrow the working set early.

- Exact-label queries are cheapest.
- Partial-label queries are fine when cardinality is bounded.
- Wide-open queries across large metrics are the most expensive path.

### Multiple store instances

If one node must serve very different workloads, separate them into different stores instead of over-tuning one global instance.

Examples:
- high-churn application metrics in one store
- long-retention infrastructure metrics in another
- isolated benchmark or test stores in memory mode

## What to tune

### `ingest_workers`

`ingest_workers` affects only HTTP imports. It controls how many background workers drain the HTTP ingest queue.

- Increase it when HTTP import is saturated and CPU is available.
- Leave it near the default for most deployments.
- Lower it for small embedded or mostly-query workloads.

### `schema`

The schema controls rollup tiers and retention. This is one of the most important operational choices because it affects both disk usage and long-range query cost.

- Short raw retention reduces storage cost for high-volume workloads.
- Longer rollup retention keeps dashboards cheap over long windows.
- Custom tiers are preferable to forcing every query through raw history.

### `raw_retention_seconds` and `daily_retention_seconds`

These are the coarse controls for storage growth.

- Increase them only when you need that data online.
- For dense operational metrics, extra raw retention adds up quickly.
- Long-term planning should usually lean on rollups, not unlimited raw data.

### `mode`

- `:disk` is the normal production path.
- `:memory` is for tests, ephemeral services, and isolated benchmarks.

## Benchmarking guidance

Use the maintained benchmark set in [`bench/README.md`](/Users/mcotner/Documents/elixir/timeless/timeless_metrics/bench/README.md).

For ingest performance, treat these as separate questions:

1. How expensive is cold population?
2. What is warm steady-state throughput?
3. How much time does flush/compression add?
4. How do real HTTP workloads compare to direct library writes?

The updated `bench/write_bench.exs` follows that split and is the best starting point for write-path analysis.

## What not to optimize first

- Do not assume compression is the bottleneck. Current compression efficiency is already strong.
- Do not compare cold-start benchmarks to warm-ingest benchmarks as if they measured the same thing.
- Do not tune legacy-only options unless you are intentionally running `engine: :legacy`.
- Do not widen queries unnecessarily when dashboards only need aggregated or filtered results.
