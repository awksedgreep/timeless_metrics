# Performance Audit — v6.1.1 / Container 0.6.4 Baselines

**Date:** 2026-07-18
**Version:** timeless_metrics 6.1.1 (Rust parser), stack image 0.6.4
**Machine:** Intel Core Ultra 9 185H ("i185"), Linux, 22 schedulers; container = rootless podman quadlet
**Purpose:** Baseline snapshot after the Rust parser swap and containerization,
and the "before" picture for the planned fused parse→resolve→write NIF.

## 1. Container HTTP ingest, batched (--batch 50, 10K series)

`realistic_workload.exs --tm-url http://127.0.0.1:8428 --batch 50`

| Interval | Req/s | Pts/s  | Write p50 | Write p99 | Query p99 (qps)  |
|----------|------:|-------:|----------:|----------:|-----------------:|
| 62ms     | 155   | 156.0K | 630us     | 1.12ms    | 1.24ms (1.2K)    |
| 31ms     | 301   | 301.6K | 562us     | 1.18ms    | 1.21ms (2.0K)    |
| 15ms     | 580   | 580.3K | 565us     | 1.25ms    | 1.27ms (2.2K)    |
| 7.0ms    | 1.1K  | 1.1M   | 581us     | 1.27ms    | 1.38ms (2.2K)    |
| 3.0ms    | 1.8K  | 1.8M   | 591us     | 1.19ms    | 1.21ms (2.2K)    |

- **Peak 1.8M pts/s** through the container's HTTP path — 3.8x the batch-1
  ceiling (477K, see 2026-07-18_container_http_workload.md)
- **Latency is flat to the top**: write p99 ~1.2ms and query p99 ~1.3ms at
  1.8M pts/s — no degradation shoulder; saturation is client-side request
  generation, not the server
- 168.6K queries, 0 errors

## 2. Rust engine baseline (embedded, 2K series x 120 pts, batch 500)

`bench/rust_engine_baseline.exs`

| Path                          | Throughput      |
|-------------------------------|----------------:|
| Raw write (pre-resolved ids)  | 3,023,964 pts/s |
| Labeled write (cached series) |   651,098 pts/s |
| Labeled write (new series)    |   141,753 pts/s |

- Flush 136ms; storage 2.473 bytes/point; single-series query 0ms;
  334-series query 6ms; aggregate 1ms

## 3. Rust query fanout baseline (12K series x 60 pts, disk)

`bench/rust_query_bench.exs`

| Query                  | Median  | Best    |
|------------------------|--------:|--------:|
| Range (12K series)     | 51.4ms  | 49.4ms  |
| Direct NIF range       | 53.4ms  | 47.1ms  |
| Aggregate (12K series) | 29.6ms  | 26.8ms  |

- Populate 535K pts/s; flush 337ms

## Fused-NIF scoreboard (targets to beat)

- Parse+terms: 7.28ms/10K samples; Elixir middle ~14ms/10K
  (2026-07-18_rust_parser_ingest_segments.md)
- Labeled cached write 651K pts/s embedded — the fused path should close
  toward the 3.0M raw ceiling
- Query medians above must not regress
