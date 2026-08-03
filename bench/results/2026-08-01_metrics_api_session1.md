# Rust metrics API POC — Session 1 server/storage contract

Session 1 establishes the standalone Rust process without adding an ingest or
query implementation. The server uses the released `timeless_metrics` virtual
table and public commands; it does not reproduce buffering, compression,
chunks, series identity, rollups, or retention.

## Implemented boundary

- Separate `poc/timeless-metrics-api` Rust workspace and descriptive binary.
- One ordered SQLite writer, configurable bounded reader pool, bounded writer
  queue, and retry of extension publication conflicts.
- Only `GET /health`, `GET /select/metrics/stats`, and ordered
  `POST /api/v1/flush`.
- Existing hourly/daily/monthly rollup ladder; 10-second flush, five-minute
  compact/rollup, hourly prune, and seven-day raw-retention policy.
- Advisory per-database owner lease and graceful ordered shutdown flush.
- Counters separating admitted/completed/failed, queued/in-flight, batches and
  points; API and maintenance phase timers; explicit index entry/byte units;
  logical payload, SQLite page/freelist, and DB/WAL/SHM physical bytes.

No auth, product route, generic three-signal layer, or host-side metrics
storage was added.

## Extension gap found by the contract

The engine correctly marked a series pending when its buffer reached 4,096
points, but none of the metrics virtual-table ingest surfaces called the
existing `flush_pending()` operation. As a result, 4,096 points remained in
memory until a host issued `flush` (normally the ten-second Elixir timer).

The fix is in `timeless-core`/`timeless-ext`, not the API. Tier 1, named batch,
resolved batch, and Prometheus ingest now drain only threshold-ready series.
The empty pending-queue path returns without a store write or retention pass.
Direct SQLite/libSQL users therefore receive the same exact automatic flush
contract as the future Rust HTTP path.

## Contract result

The extension-backed test proves:

1. one named-batch series has 4,095 buffered points, zero disk points, and zero
   raw chunks;
2. the next point produces zero buffered points, 4,096 disk points, and one raw
   chunk without an API/host flush;
3. a ten-point tail plus ordered HTTP flush reports a three-batch/4,106-point
   watermark with no queued, in-flight, failed, or missing work;
4. shutdown/reopen recovers one series and all 4,106 points;
5. a second API owner is rejected and no Session 2 ingest route exists.

The direct extension correctness suite separately pins the 4,095/4,096
transition and transaction/savepoint/maintenance rollback behavior.

## Shell smoke measurement

Environment matches the Session 0 host. This is an empty release server with
two readers and sequential loopback requests, not the Session 0 ingest
workload.

| route | requests | errors | sequential req/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|
| `GET /health` | 2,000 | 0 | 2,047.2 | 471.2us | 788.8us | 914.1us |
| `GET /select/metrics/stats` | 2,000 | 0 | 2,007.9 | 490.5us | 790.2us | 914.6us |
| `POST /api/v1/flush` | 200 | 0 | 4,131.6 | 214.4us | 453.2us | 560.3us |

Linux process `VmHWM` and final `VmRSS` were both 9,176 KiB. The high virtual
mapping (`VmPeak` 1,702,544 KiB) reflects the configured SQLite mmap ceiling,
not resident memory.

There is intentionally no completed-points throughput comparison yet: adding
a benchmark-only ingest route in Session 1 would violate the POC sequence.
Session 0's 777.0-792.7K completed points/s Elixir+libSQL control remains the
comparison for Session 2's native batched ingest.

## Validation

```text
cargo test -p timeless-ext                         33 passed
tests/correctness.sh r1                            passed
cargo clippy --all-targets -- -D warnings          passed
cargo test (timeless-metrics-api)                  5 passed, 1 ignored
storage_contract with release extension            passed
```

Session 1 meets its exit criterion. Session 2 can add the pinned Prometheus and
VictoriaMetrics HTTP ingest contracts without changing this storage boundary.
