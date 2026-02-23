# Timeless TODO

## Performance — Retention

### Benchmarked: Secondary indexes on end_time/bucket HURT performance
Tested with `mix bench_retention` — indexes made DELETE **3-4x slower**, not faster.

**Why:** Retention deletes a large fraction of data (oldest 33%). For bulk deletes,
SQLite's sequential scan of a WITHOUT ROWID clustered B-tree is faster than
index-assisted random I/O + the overhead of maintaining the secondary index on
every INSERT. Indexes only help for highly selective queries (deleting <5% of rows).

Results at 10K series / 30 days (3.6M raw segments, 7.2M tier rows):
- WITH indexes:    3.69s total (1.34s raw + 2.34s tier)
- WITHOUT indexes: 1.07s total (0.55s raw + 0.53s tier)

**Decision:** No secondary indexes. The full table scan is the right approach.
At 10K series / 30 days, 1 second for full retention enforcement is fine.

## Future — Time Partitioning (if needed beyond 100K devices)
Weekly table partitioning as a scaling path:
- `raw_segments_2026_w05`, `raw_segments_2026_w06`, etc.
- Writes go to current week's table
- Queries fan out across tables covering the requested time range
- Retention = `DROP TABLE` on expired weeks — instant, O(1), pages to freelist
- Typical dashboard queries (15m-7d) hit 1-2 tables, 90d query hits ~13
- Only worth the complexity at 100K+ devices × 100 metrics (10M+ series)

Projected retention cost at scale (extrapolating from bench):
- 10K series / 30 days: ~1s (benchmarked)
- 100K series / 90 days: ~30-90s (linear in segment count)
- 1M series / 90 days: table partitioning likely needed

## Prometheus Scraper — Deferred Improvements

### Counter Reset Detection
- Detect counter resets at ingest time (value decreases → reset)
- Not needed now — `rate()` handles resets at query time
- Would enable `increase()` to be more accurate across long ranges

### Stale Marker Handling
- When a target disappears a series, inject NaN sentinel value
- Allows dashboards to show "no data" instead of flat-lining at last value
- Prometheus uses special NaN bit pattern for staleness

### TYPE/HELP Metadata Extraction
- NIF parser currently skips `# TYPE` and `# HELP` comment lines
- Could extract and populate `metric_metadata` table automatically
- Would auto-register gauge/counter/histogram/summary types

### Histogram Bucket Semantic Awareness
- Prometheus histograms use `_bucket{le="..."}`, `_sum`, `_count` convention
- Could auto-detect histogram families and provide native histogram queries
- Would enable `histogram_quantile()` PromQL function
