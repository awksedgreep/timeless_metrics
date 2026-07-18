# Benchmark Results — Containerized Stack HTTP Ingest (First Container Baseline)

**Date:** 2026-07-18
**Image:** ghcr.io/awksedgreep/timeless-stack:0.6.4 (timeless_metrics 6.1.1, Rust parser)
**Runtime:** rootless podman quadlet, published ports, bind-mounted /data
**Workload:** 500 devices × 20 metrics (10K series), auto-ramp ÷2 from 4s interval, 20 query workers ramped alongside writes
**Benchmark script:** `bench/realistic_workload.exs --tm-url http://127.0.0.1:8428 --vm-url "" --devices 500 --metrics 20`
**Client:** separate host BEAM (pure HTTP), Intel Core Ultra 9 185H ("i185"), 22 schedulers
**Batch:** 1 device/POST (20 pts/request) — **HTTP-request-bound configuration**

## Summary

- **Peak: 477.2K pts/s** at 7ms interval (~24K HTTP req/s), graceful plateau
- 142K queries served during ramp, **0 errors**; data verified (10.1K series)
- Container memory after full run: **347MB RSS** (metrics + logs + traces + UI)

## Write Latency

| Interval | Req/s | Pts/s  | p50    | p99    | p999   |
|----------|------:|-------:|-------:|-------:|-------:|
| 4.0s     | 124   | 2.5K   | 870us  | 1.29ms | 1.39ms |
| 1.0s     | 498   | 10.0K  | 587us  | 1.16ms | 1.39ms |
| 250ms    | 2.0K  | 39.8K  | 523us  | 794us  | 1.12ms |
| 62ms     | 7.9K  | 157.4K | 351us  | 672us  | 13.4ms |
| 31ms     | 15.4K | 307.5K | 336us  | 768us  | 1.22ms |
| 15ms     | 23.6K | 472.8K | 4.24ms | 9.18ms | 11.6ms |
| 7.0ms    | 23.9K | 477.2K | 11.7ms | 17.3ms | 26.0ms |

## Query Latency Under Write Load

| Write Pts/s | Q/s  | p50    | p99    | p999   |
|-------------|-----:|-------:|-------:|-------:|
| 10.0K       | 97   | 658us  | 1.23ms | 1.84ms |
| 157.4K      | 1.2K | 418us  | 752us  | 7.41ms |
| 307.5K      | 2.0K | 433us  | 871us  | 1.44ms |
| 477.2K      | 943  | 11.9ms | 17.7ms | 39.6ms |

## Interpretation

- **Clean zone ≈ 300K pts/s at this cardinality**: write and query p99 both
  sub-millisecond while running concurrently
- Saturation is HTTP handling (24K req/s at 20 pts/request), not the storage
  engine — rerun with `--batch 50` for the engine-side ceiling
- Rootless container overhead vs earlier host-process runs: negligible
