# Benchmarking an Elixir Timeless Database Against VictoriaMetrics

We built [Timeless](https://github.com/awksedgreep/timeless_metrics), an embedded time-series database written entirely in Elixir, running on the BEAM virtual machine. This post covers how we benchmarked it against VictoriaMetrics — a production Go-based TSDB — and what we learned about write throughput, query performance, and storage efficiency on identical hardware.

We can either embed into your Phoenix app and handle metrics/logs/traces for you with dashboards, or run indenpendently as a container or on a standalone server.  One advantage of the BEAM is supervision trees and the ability to run all three on one server/container safely.  If your app crashes, the other services will continue working just fine.  You can even catch and log criticalerrors within your app to your built in log server.

## The Setup

Both databases ran on AWS i8g.24xlarge instances (96 vCPU ARM, 768 GiB RAM) with a separate identical instance as the load generator. We used a custom Elixir-based benchmark harness that generates realistic metric workloads: 200,000 unique time series across 10,000 simulated devices with 20 metrics each.

Each HTTP request carries a batch of 50 data points. The harness ramps 256 concurrent writers during a 120-second warmup period, then measures for 300 seconds with concurrent writes and 24 query workers (~10:1 write-to-read ratio, reflecting typical production workloads).

Queries use PromQL `query_range` requests against random series with a 1-hour window and 60-second step — the typical dashboard query pattern.

Both databases were tested under identical conditions: same hardware, same harness, same series count, same concurrency, same measurement window.

### What We Measured

- **Write throughput**: points per second sustained over the measurement period
- **Write latency**: average HTTP round-trip per write request
- **Query throughput**: PromQL query_range requests per second
- **Query latency**: average HTTP round-trip per query request
- **Storage efficiency**: bytes per point on disk after compression
- **Stability**: behavior under sustained load over minutes

## Architecture

Timeless uses a sharded architecture designed for the BEAM's concurrency model:

- **Sharded ETS write buffers** — incoming points land in one of N lock-free ETS tables (N = half the CPU count, so 48 on our 96-core test machine). Writes are pure ETS inserts with no GenServer in the hot path. Each shard is selected by series ID, distributing lock contention across tables.
- **Parallel SegmentBuilder workers** — each shard has a paired GenServer that accumulates points in memory and periodically compresses them using Gorilla encoding (delta-of-delta timestamps + XOR float values) followed by zstd compression. Compression runs asynchronously in Tasks to avoid blocking the write path.
- **Rocket HTTP server** — a custom HTTP server using OTP 28's `:socket` module and a picohttpparser NIF for request parsing, bypassing the overhead of a general-purpose web framework.
- **NIF-accelerated parsing** — Prometheus text format is parsed by a C++ NIF. JSON import uses OTP 28's built-in `:json.decode` NIF.
- **Lock-free series creation** — new series get IDs from an `:atomics` counter and register via `:ets.insert_new` (atomic compare-and-swap). SQLite metadata writes are batched asynchronously. This eliminates the GenServer bottleneck during high-cardinality series creation.

VictoriaMetrics uses a Go-based LSM-tree architecture with its own custom compression format (delta-of-delta + XOR, similar to Gorilla but with custom optimizations).

## Results

Both databases were tested with 256 concurrent writers and 24 query workers on the same hardware:

| Metric | Timeless | VictoriaMetrics |
|--------|----------|-----------------|
| **Write rate** | 4.39M pts/s | 4.47M pts/s |
| **Write latency** | 2.94ms | 2.85ms |
| **Write errors** | 0 | 0 |
| **Query rate** | **7,853 q/s** | 3,045 q/s |
| **Query latency** | **3.06ms** | 7.88ms |
| **Storage** | **0.7 bytes/pt** | unable to measure(crashes) |

### Write Performance

Timeless matches VictoriaMetrics on write throughput — 4.39M vs 4.47M points per second, 2% or within the margin of error. Write latency is nearly identical at ~3ms. Both databases handled the full 300-second measurement period with zero write errors at this configuration.

For an Elixir application running on the BEAM to match a mature Go database on raw write throughput is notable. The key is that the hot write path stays in lock-free ETS (implemented in C inside the BEAM VM) and uses NIFs for parsing — Erlang process scheduling is not in the critical path.

### Query Performance

This is where Timeless pulls ahead. Under identical concurrent write-and-read load, Timeless serves 2.6x more queries per second (7,853 vs 3,045) at less than half the latency (3.06ms vs 7.88ms).

The sharded architecture distributes query load across independent SegmentBuilder workers, each reading from its own file-based storage without cross-shard contention. The query path reads directly from Gorilla-compressed segments via lock-free file operations — no GenServer is involved in the read path, so queries don't compete with writes for process mailbox time.

The BEAM's lightweight process model is an advantage here. Each query runs in its own process without thread pool contention, and the preemptive scheduler ensures no single query starves other work.

### Storage Efficiency

We measured Timeless's on-disk storage from a separate sustained ingest test (256 writers, 300-second measurement, 1.13 billion points ingested):

- **Timeless**: 739 MB for 1.13 billion points = **0.7 bytes per point** (Gorilla + zstd)

This is an excellent compression ratio — under 1 byte per data point including timestamps. The two-stage pipeline (Gorilla delta-of-delta encoding followed by zstd compression) achieves roughly 23x compression over the raw 16-byte representation (8-byte timestamp + 8-byte float).

We attempted to measure VictoriaMetrics storage under equivalent sustained load, but the ARM build (v1.108.1) crashed during internal compaction at every concurrency level we tested (16, 32, 64, and 256 writers). From the data VM managed to ingest before crashing (321M points), we measured 2.6 GB on disk — but this was uncompacted data. A fully compacted VictoriaMetrics dataset typically achieves 1-2 bytes per point based on published benchmarks.

## What We Learned

### The BEAM Can Compete

Go is a VERY fast language.  Elixir is a fast language as well.  I've proven with a few NIFs for critical operations, Elixir could compete with Go on raw throughput.  Stability is free with Elixir too.  It easily scales to the number of cores you have available and works equally well as embedded as it does as a standalone server with 96 cores.

### Architecture Matters More Than Language

Using sharded ETS architecture — the same fundamental design pattern that VictoriaMetrics uses with its LSM tree shards.  

### Stability Under Load

During our storage measurement tests, we discovered an interesting architectural difference. When we re-tested VictoriaMetrics with data that required actual storage and compaction, VM crashed under the same write load it had previously handled — its internal compaction could not keep pace with sustained ingestion, causing the process to panic during merge operations. This happened consistently across multiple concurrency levels.

Timeless includes backpressure mechanisms that keep the system stable under sustained load. In the comparison run, these never activated — Timeless absorbed the full write load with zero errors. The backpressure exists as a safety net, not as normal operating behavior.

## Methodology Notes

- **Hardware**: AWS i8g.24xlarge (96 vCPU Graviton3, 768 GiB RAM), separate load generator of the same instance type
- **Network**: Same VPC, private IP communication, sub-millisecond RTT
- **Timeless version**: sharded-engine-revival branch, Elixir 1.19+ on OTP 28
- **VictoriaMetrics version**: v1.108.1 ARM64 binary (December 2024)
- **Workload**: 200,000 series (10,000 devices × 20 metrics), batches of 50 points per HTTP request, random float values 0-100
- **Concurrency**: 256 writers + 24 query workers for both databases
- **Write format**: Prometheus text exposition (Timeless), JSON line import (VictoriaMetrics — Prometheus text endpoint had issues on ARM)
- **Query format**: PromQL query_range, 1-hour window, 60-second step, random series selection
- **Measurement period**: 120-second warmup (writes only) + 300-second measurement (writes + queries)
- **Source code**: Benchmark harness, playbooks, and all configuration available at [github.com/awksedgreep/timeless_bench](https://github.com/awksedgreep/timeless_bench)

The code is open source. We welcome benchmarks, bug reports, and pull requests.
