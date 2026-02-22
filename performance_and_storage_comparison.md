# TimelessMetrics vs VictoriaMetrics: Performance & Storage Comparison

Last updated: 2026-02-21

## Test Environment

- CPU: 22 cores (AMD)
- Storage: tmpfs (ramdisk) — eliminates disk I/O as a variable
- TimelessMetrics: Elixir/OTP, Gorilla NIF + zstd level 9, SQLite backend
- VictoriaMetrics: Go, custom decimal-delta + ZSTD, LSM-tree storage
- Data: 50 devices x 10 metrics x 30 days @ 5-min intervals = 4.3M points
- Series: 500 (realistic ISP monitoring data — noisy sine waves, gauges, counters)

## Ingest Throughput

| Method | pts/sec | Notes |
|--------|---------|-------|
| **TM native write_batch** (22 concurrent writers) | **3.2M** | In-process Elixir API with series index lookup |
| **TM native wall** (write + flush + zstd compress) | **933K** | Includes synchronous Gorilla + zstd-9 compression |
| **TM HTTP** (Prometheus text format, synchronous) | **921K** | Parse + index + write, response after data committed |
| **VM HTTP accept** (Prometheus text, async response) | **9.7M** | Returns 204 before data is indexed — misleading |
| **VM HTTP sustained** (send + wait for data queryable) | **3.1M** | Actual throughput after verifying all 500 series land |

### Key Findings

- **TM native vs VM HTTP sustained: 1.03x** — effectively parity. Elixir's embedded
  API with index lookup matches Go's optimized HTTP server when measured honestly.
- VM's "accept" rate (9.7M) is meaningless — it returns HTTP 204 before processing.
  Real sustained rate is 3.1M after waiting for data to be queryable.
- TM HTTP (921K) still 3.4x behind VM HTTP sustained (3.1M). The gap is in parsing
  overhead — TM parses Prometheus text synchronously in the request handler.
- The old vs_victoriametrics benchmark was reporting 441K for TM because it lumped
  flush+compress time into ingest timing while VM deferred compression to background.

### From `mix bench` (100 devices x 20 metrics, 51.8M points)

| Method | pts/sec |
|--------|---------|
| Single series write | 499K |
| Single series, pre-resolved (no index lookup) | 572K |
| Batch write (with index lookup) | 1.5M |
| Batch write, pre-resolved | 2.8M |
| Concurrent batch (22 writers, with index) | 1.1M |
| Concurrent batch, pre-resolved (22 writers) | **9.8M** |

Pre-resolved writes skip the series registry ETS lookup entirely. This is the path
for embedded users who cache their series IDs.

## Storage Efficiency

### Same 4.3M Point Benchmark

| | Total Size | Bytes/Point | Notes |
|---|---|---|---|
| **TimelessMetrics** | **41.5 MB** | **9.23** | Gorilla NIF + zstd-9, 4h segments (48 pts/seg) |
| **VictoriaMetrics** | **753.4 MB** | **174** | Post-compaction (forced merge), includes index |

**TM uses 18x less disk for the same data.**

### VM Disk Breakdown (Post-Compaction)

```
data/small/ (time series data):  676.7 MB
data/indexdb/ (inverted index):   76.7 MB
data/big/:                        12 KB
Total:                           753.4 MB
```

### Why VM Uses So Much Space on This Data

VictoriaMetrics' compression pipeline converts floats to decimal integers, then
applies delta or delta-of-delta encoding + ZSTD. This works brilliantly for:
- Integer counters (delta-of-delta ≈ 0 → ZSTD crushes it)
- Stable gauges (small deltas → efficient varint encoding)
- Mostly-constant values (classified as `MarshalTypeConst` → 0 bytes)

Our benchmark data is **their worst case**: noisy floating-point values where every
sample differs significantly. The decimal conversion produces large integers with
high-entropy deltas that neither delta encoding nor ZSTD can compress.

Additionally:
- 500 series is very low — VM's LSM-tree (mergeset) index overhead doesn't
  amortize. The 76.7 MB indexdb stores per-day inverted index entries per series.
- Write amplification from LSM merge stages inflates `data/small/` even post-merge.

### VM's "0.7 Bytes/Point" Claim

VictoriaMetrics publishes various bytes/point figures:

| Claim | Context |
|-------|---------|
| 0.4 bytes/pt | node_exporter data (blog post by valyala) |
| 0.5 bytes/pt | "Production workload" (Promscale comparison article) |
| ~1 byte/pt | Their own capacity planning guide default |
| 0.8 bytes/pt | VictoriaMetrics Cloud pricing docs |

**These are marketing-optimized best-case numbers.** Their own maintainer (Roman
Khavronenko, GitHub issue #4374) says: *"Such formula would require data
details...each installation is unique — this formula doesn't exist."*

Critical caveats:
- **Index is excluded.** Their docs say index typically adds 20%, but for low-cardinality
  setups (like ours) it can exceed 50%.
- The 0.4 claim is specifically node_exporter: integer counters, mostly constant
  between scrapes, perfectly regular timestamps.
- At scale (millions of series), per-series overhead amortizes. At 500 series, it doesn't.

### TM Bytes/Point by Segment Duration

Segment duration significantly affects compression — more points per segment means
Gorilla's XOR encoding has more runway and per-segment overhead is amortized:

| Segment Duration | Points/Segment | Segments | Bytes/Point |
|---|---|---|---|
| 4h (benchmark default) | 48 | 180 | 10.36 |
| 24h | 288 | 30 | 7.89 |
| 7d | 2,016 | 5 | 7.46 |

Tested on single-series CPU data (30 days, noisy sine wave).

### TM Bytes/Point by Data Pattern

From `mix bench` compression analysis (10K points each, 4h segments):

| Pattern | Bytes/Point |
|---------|-------------|
| Constant (42.0) | 0.01 |
| Counter (monotonic) | 2.29 |
| Sine wave | 9.51 |
| Random (uniform) | 23.82 |
| Spiky (mostly flat) | 24.18 |

Gorilla excels at constant/counter data. Random floats are worst-case — XOR
encoding between unrelated float64 bit patterns produces full-width differences.

## Query Latency

50 iterations each, data from 4.3M point benchmark:

| Query | TimelessMetrics | VictoriaMetrics | Ratio |
|-------|----------------|-----------------|-------|
| raw 1h (single series) | 129 us | 375 us | **2.9x TM wins** |
| raw 24h (single series) | 273 us | 472 us | **1.7x TM wins** |
| agg 1h avg (single) | 125 us | 246 us | **2.0x TM wins** |
| multi 50 hosts 1h | 3.57 ms | 447 us | **0.1x VM wins** |
| latest value | 151 us | 194 us | **1.3x TM wins** |

TM wins 4/5 query types. VM wins multi-series queries because its inverted index
is optimized for fan-out across many series, while TM queries series sequentially.

## VictoriaMetrics Compression Internals

VM does **not** use Gorilla. Their pipeline:

1. **Float → decimal integer**: `FromFloat()` converts `1234.56` → `(v=123456, e=-2)`.
   Strips trailing zeros. Avoids IEEE 754 float bit entropy entirely.
2. **Block classification** (up to 8,192 points per block):
   - `MarshalTypeConst`: all values identical → 0 bytes for values
   - `MarshalTypeDeltaConst`: arithmetic progression → 1 varint
   - `MarshalTypeZSTDNearestDelta`: gauge data → delta + ZSTD
   - `MarshalTypeZSTDNearestDelta2`: counter data → delta-of-delta + ZSTD
3. **Varint encoding**: zigzag LEB128 for signed deltas
4. **ZSTD**: levels 1-5 based on block size (>=128 bytes), skipped if <10% savings
5. **Optional lossy compression**: `precisionBits` can zero trailing bits for smaller deltas

Their per-block overhead is ~81 bytes (TSID, timestamps, offsets, sizes). The
inverted index (indexdb) uses a mergeset (LSM-tree variant) with per-day entries
per series — roughly 700+ bytes per series per day before mergeset overhead.

## TimelessMetrics Configuration Reference

### Compression Options

| Option | Default | Description |
|--------|---------|-------------|
| `compression` | `:zstd` | `:none`, `:zlib`, `:zstd`, `:auto` |
| `compression_level` | `9` | zstd level 1-22 (9 = good balance) |

Set in `TimelessMetrics.Supervisor.start_link/1`:
```elixir
{TimelessMetrics, name: :metrics, data_dir: "/data",
  compression: :zstd, compression_level: 9}
```

**Tradeoffs:**
- `:none` — fastest ingest, largest storage. Good for benchmarking raw write speed.
- `:zlib` — always available, moderate compression. ~70% of zstd ratio.
- `:zstd` level 1-3 — fast compression, slightly worse ratio than level 9.
- `:zstd` level 9 (default) — good balance of speed and ratio.
- `:zstd` level 15+ — diminishing returns, significant CPU cost.

### Segment Duration

| Option | Default | Description |
|--------|---------|-------------|
| `segment_duration` | `14_400` (4h) | Seconds per Gorilla segment |

**Impact:** Larger segments = better compression (more points for XOR encoding to
work with, less per-segment overhead). But larger segments mean higher query latency
for recent data (must decompress entire segment to read tail).

| Duration | Points @ 5-min | Use Case |
|----------|---------------|----------|
| 3,600 (1h) | 12 | Low-latency edge monitoring |
| 14,400 (4h) | 48 | General purpose (default) |
| 86,400 (24h) | 288 | Storage-optimized, batch workloads |
| 604,800 (7d) | 2,016 | Archival, maximum compression |

### Buffer Tuning

| Option | Default | Description |
|--------|---------|-------------|
| `buffer_shards` | `max(schedulers/2, 2)` | Number of parallel write buffers |
| `flush_interval` | `5_000` ms | Timer-based flush period |
| `flush_threshold` | `10_000` points | Point count trigger per shard |
| `pending_flush_interval` | `60_000` ms | WAL write interval for queryability |

**Tradeoffs:**
- Higher `buffer_shards` = better write parallelism but more memory.
- Higher `flush_threshold` = fewer, larger flushes (better compression batching).
  The `mix bench` uses 200,000 for bulk ingest.
- Lower `flush_interval` = fresher data queryable, more flush overhead.

### Schema (Retention & Tiers)

```elixir
schema = %TimelessMetrics.Schema{
  raw_retention_seconds: 7 * 86_400,        # 7 days raw data
  rollup_interval: :timer.minutes(5),        # rollup tick frequency
  retention_interval: :timer.hours(1),       # retention cleanup frequency
  tiers: [
    %Tier{name: :hourly,  resolution_seconds: 3_600,     retention_seconds: 30 * 86_400},
    %Tier{name: :daily,   resolution_seconds: 86_400,    retention_seconds: 365 * 86_400},
    %Tier{name: :monthly, resolution_seconds: 2_592_000, retention_seconds: :forever}
  ]
}
```

### Environment Variables (Production)

| Variable | Default | Description |
|----------|---------|-------------|
| `TIMELESS_DATA_DIR` | `/data` | Storage directory |
| `TIMELESS_PORT` | `8428` | HTTP server port |
| `TIMELESS_SHARDS` | `schedulers_online()` | Buffer shard count |
| `TIMELESS_SEGMENT_DURATION` | `14400` | Segment duration in seconds |
| `TIMELESS_PENDING_FLUSH_INTERVAL` | `60` | Pending flush interval in seconds |
| `TIMELESS_BEARER_TOKEN` | (none) | HTTP authentication token |

## Areas for Improvement

### 1. HTTP Parsing (Current Gap: 3.4x vs VM)

TM HTTP does 921K pts/sec vs VM's 3.1M sustained. The bottleneck is serial Prometheus
text parsing in the HTTP request handler. Current optimizations (parallel parsing for
batches >2K lines, binary matching, fused zip) brought this from ~400K to ~920K.

**Next steps:**
- NIF-based Prometheus line parser — parsing `metric{k="v"} 1.23 1700000000\n` in C
  would eliminate the Elixir string splitting/float parsing overhead.
- Streaming body parsing — currently `read_body` loads the entire body, then splits.
  Parsing as the body streams in would reduce peak memory and allow pipelining.
- Connection keep-alive tuning — Bandit defaults may not be optimized for bulk ingest.

### 2. Multi-Series Query (Current Gap: 8x vs VM)

TM takes 3.57ms for 50-host fan-out vs VM's 447us. TM queries series sequentially;
VM's inverted index is optimized for this pattern.

**Next steps:**
- Parallel series decompression — query all 50 series concurrently with Task.async.
- Pre-grouped segment reads — batch SQLite reads for segments in the same time range.
- In-memory segment cache — hot segments (recent data) kept decompressed.

### 3. Storage Efficiency for Noisy Float Data

9.23 bytes/point on noisy ISP data. The per-segment overhead at 48 points/segment
(4h @ 5-min intervals) is the main driver — drops to 7.46 at 7-day segments.

**Next steps:**
- Adaptive segment duration — auto-size segments based on write rate, not wall clock.
  High-frequency series get larger segments naturally.
- Decimal integer pre-processing (VM's approach) — for metrics that happen to be
  round numbers (integer counters, percentage gauges), converting to decimal integers
  before Gorilla XOR encoding could reduce bit-width of differences.
- Dictionary-based ZSTD — train a dictionary on typical metric data, share across
  segments. Particularly effective for small segments where ZSTD has limited context.

### 4. Sustained Load Backpressure

The lazy backpressure check (atomics-cached, 100ms TTL) reduces Process.info calls
but the mechanism is still coarse. Under sustained load:

**Next steps:**
- Adaptive flush thresholds — increase flush_threshold when write rate is high to
  batch more points per compression pass.
- Write-ahead coalescing — batch multiple write_batch calls into a single ETS insert
  when the caller is an HTTP handler processing a large body.

### 5. Benchmark Infrastructure

The vs_victoriametrics benchmark now correctly:
- Separates write time from flush+compress time
- Uses concurrent writers matching `mix bench`
- Pre-builds payloads to measure server throughput, not client serialization
- Verifies VM data is actually queryable before stopping the clock
- Reports both "accept" and "sustained" rates for VM

**Still needed:**
- Concurrent HTTP client benchmark (multiple Req connections posting in parallel)
  to test TM HTTP under realistic multi-client load.
- Vary series count (500, 5K, 50K) to test scaling behavior.
- Test with real Prometheus scrape data (node_exporter) for apples-to-apples with
  VM's published benchmarks.
- Test on real disk (not tmpfs) to include I/O latency.
