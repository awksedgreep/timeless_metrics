# Packed rollup adapter result — 2026-07-31

TimelessMetrics now executes one prepared `timeless_rollup_batches` statement
and one `TRB1` decode for `query_daily/5`. This replaces six read barriers, six
row-TVF scans, repeated label JSON decoding, and an Elixir timestamp join.

## Public boundary workload

Command:

```console
MIX_ENV=test mix run bench/rollup_query_bench.exs --runs 15 --buckets 1200
```

The fixture is one exact-label series with 1,200 settled daily buckets. The old
operation in the harness reproduces the removed adapter path; the new operation
calls the public `TimelessMetrics.query_daily/5`. Warm results must be exactly
equal before sampling.

| Path | Median | p95 | Buckets |
|---|---:|---:|---:|
| Former six-query adapter | 14.744 ms | 16.982 ms | 1,200 |
| Packed public adapter | 0.767 ms | 0.849 ms | 1,200 |

Median latency is **19.22× lower**. Fixture setup took 3.378 ms to ingest,
0.519 ms to flush, and 1.846 ms to build the declared rollup tiers; setup is
outside query samples.

## Coverage

- The packed and former row-oriented results are exactly equal before timing.
- Restart recovery is exercised before the primary `query_daily` regression.
- Count remains an integer beyond `2^53`; malformed lengths and unknown
  versions fail loudly.
- Existing rollup-retention coverage continues through the same test.
- Each pooled reader owns one prepared packed statement; the writer fallback
  uses the same public SQL shape.

The paired extension-only result records the 60,000-bucket multi-series gate.
