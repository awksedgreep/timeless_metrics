# Rust Engine Optimization Notes

These are focused follow-ups from a June 2026 review of the Rust engine hot paths.

## Candidates

1. Deduplicate pending flush queue entries for hot partitions.
   A partition that has already crossed `flush_threshold` should not push its key into the global flush queue on every subsequent write before the periodic flush drains it. This keeps the queue bounded under hot-series workloads without changing storage format or query semantics.

2. Start series matching from the smallest candidate set.
   `find_series/2` currently starts from all series for a metric, then intersects label filters. For high-cardinality metrics with selective labels, choose the smallest set among the metric index and label indexes first.

3. Read only headers during index rebuild.
   Startup index rebuild currently reads whole `.pco1` and `.pcb1` files to parse metadata. Large stores would benefit from bounded header reads and seeks.

4. Revisit the Rust labeled-write cache only if labeled writes become the main API path.
   The public Elixir write path resolves through ETS and calls the raw binary NIF. The Rust labeled cache verifies cache hits under the registry read lock, so it is not currently the first place to optimize.

