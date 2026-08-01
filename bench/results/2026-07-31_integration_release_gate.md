# libSQL integration and default-engine release gate — 2026-07-31

This is the Session 8 result for the extension-first query-performance plan.
The fixed 20% query-p95 gate does **not** pass, so `:rust` remains the default
engine. The result is a release decision, not a benchmark victory claim.

## Reproduction

Starting TimelessMetrics revision:
`d355d3d1ed43ad2f77e90e47b7a3455d57ba9d6b` on
`feat/libsql-storage-engine`, with the migration work uncommitted. The paired
extension revision was `a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69` on
`feat/timeless-metrics-embedding`, also dirty.

```sh
bench/engine_query_distribution.sh 5 5
MIX_ENV=test mix run --no-compile bench/write_bench.exs --scale large
MIX_ENV=test mix run --no-compile bench/write_bench.exs --libsql --scale large
for process in 1 2 3 4 5; do
  MIX_ENV=test mix run --no-compile bench/rollup_query_bench.exs \
    --runs 15 --buckets 1200
done
MIX_ENV=test mix run --no-compile bench/libsql_soak.exs --seconds 30 --readers 8
```

Environment: Linux 7.1.3 x86-64, Intel Core Ultra 9 185H, 22 BEAM
schedulers, Elixir 1.20.2, OTP 29, bundled SQLite 3.53.2, `/tmp` on tmpfs,
and the CPU governor left at `powersave`. Engines ran sequentially, never
against each other. The local paired NIFs were compiled from the dirty source
trees before measurement.

## Fresh-process query distribution

Each engine ran in five fresh BEAM processes. Within each process, exact and
discovery p95 use 100 samples; the other query shapes use five samples. The
table reports the median of the five process-level p95 values and the complete
process range. This prevents one favorable process from deciding the gate.

| Public query | Rust p95 median (range) | libSQL p95 median (range) | Gate result |
|---|---:|---:|---:|
| First exact after flush | 1.573 ms (1.053–1.922) | 2.364 ms (1.994–2.524) | fail: +50.3% |
| Exact raw | 0.209 ms (0.171–0.264) | 0.570 ms (0.468–0.717) | fail: +172.7% |
| Narrow raw, 188 series | 1.442 ms (1.245–1.572) | 2.253 ms (1.873–2.444) | fail: +56.2% |
| Wide raw, 12K series | 84.201 ms (79.942–85.195) | 114.366 ms (103.365–117.521) | fail: +35.8% |
| Selective regex raw | 76.538 ms (73.425–81.206) | 3.641 ms (3.128–5.114) | pass: libSQL 21.0x faster |
| Selective regex discovery | 27.642 ms (26.300–28.335) | 3.198 ms (2.691–4.138) | pass: libSQL 8.64x faster |
| Negative raw, 6K series | 74.102 ms (70.824–82.491) | 60.546 ms (58.919–64.792) | pass: libSQL 18.3% faster |
| Scalar average, 12K series | 30.884 ms (27.652–32.129) | 43.818 ms (39.068–83.078) | fail: +41.9% |
| Bucketed average, 72K buckets | 131.010 ms (127.509–133.403) | 144.622 ms (130.878–172.701) | pass: +10.4% |
| Exact latest | 0.189 ms (0.172–0.210) | 0.398 ms (0.313–0.666) | fail: +110.6% |
| Latest, 12K series | 83.769 ms (81.863–86.606) | 42.408 ms (41.333–76.464) | pass: libSQL 1.98x faster |

The five raw process-level p95 samples, in run order, were:

| Query | Rust p95 samples (ms) | libSQL p95 samples (ms) |
|---|---|---|
| Exact raw | 0.211, 0.264, 0.209, 0.171, 0.172 | 0.570, 0.473, 0.717, 0.604, 0.468 |
| Narrow raw | 1.464, 1.572, 1.442, 1.270, 1.245 | 2.444, 2.292, 1.873, 2.253, 1.931 |
| Wide raw | 83.534, 84.201, 84.534, 79.942, 85.195 | 114.366, 117.521, 110.994, 116.984, 103.365 |
| Scalar average | 27.652, 29.881, 30.970, 32.129, 30.884 | 41.866, 43.818, 44.753, 83.078, 39.068 |
| Bucketed average | 128.956, 133.403, 131.010, 127.509, 131.314 | 144.622, 148.686, 130.878, 172.701, 134.206 |
| Exact latest | 0.189, 0.210, 0.184, 0.193, 0.172 | 0.313, 0.666, 0.395, 0.447, 0.398 |
| Latest multi | 86.551, 86.606, 81.863, 82.507, 83.769 | 42.408, 42.408, 76.464, 43.212, 41.333 |

The Rust and libSQL wide-query worker peak increments were stable at 8.406x
and 9.306x their serialized results respectively; both remain within the 10x
memory bound. The libSQL scalar and window outliers in process four are kept in
the distribution rather than discarded.

## Large write and storage gate

The original large workload populated 10K series with 10M points, then ran
three separate 10-second steady phases. Batch calls contain 100 points.

| Measurement | Rust blocks | libSQL | Result |
|---|---:|---:|---:|
| Cold population | 978.8K points/s | 1.23M points/s | libSQL 1.26x faster |
| Single writes | 469.9K points/s | 105.6K points/s | Rust 4.45x faster |
| 22 concurrent single writers | 435.9K points/s | 107.9K points/s | Rust 4.04x faster |
| 100-point batches | 1.24M points/s | 1.32M points/s | libSQL 6.5% faster |
| Flush and compression | 3.219 s | 1.301 s | libSQL 2.47x faster |
| Storage | 2.304 bytes/point | 1.614 bytes/point | libSQL 29.9% smaller |
| Warm 1,003-point query p95 | 58 us | 146 us | fail: libSQL 2.52x slower |

Compared with the stored pre-query-work baseline, libSQL batch ingest changed
from 1.41M to 1.32M points/s (-6.4%) and storage changed from 1.586 to 1.614
bytes/point (+1.8%). Both stay inside the 10% regression bound and retain an
advantage over the Rust engine. Single-write throughput remains a known libSQL
weakness but is not a regression introduced by the read work.

## Rollup result

The hot Rust block engine has no stored-rollup query implementation:
`RustEngine.rollup/1` is a no-op and `query_daily/5` remains a legacy-tier
capability. There is therefore no honest Rust p95 denominator for this shape.
This is a libSQL capability gain, not a query-gate pass inferred from missing
data.

For the public libSQL path, five fresh processes compared 1,200 packed daily
buckets with the former six-query adapter. The packed median was 0.815 ms
(0.722–0.926 ms) and its p95 median was 1.190 ms (0.790–1.239 ms). The former
adapter measured 14.971 ms median and 16.704 ms p95, making the current path
18.37x faster by median and 14.04x faster by p95.

## Mixed soak

`bench/libsql_soak.exs` keeps an immutable oracle corpus under exact, narrow,
wide, regex, scalar, window, latest, and discovery reads while a separate
metric receives live batches. It concurrently flushes, applies retention,
compacts, rolls up, and creates online backups; kills the writer once; then
checks the oracle, rollups, and retention in the live store, a restored backup,
and the reopened primary store.

The final full 30-second run completed with:

```text
writes=392512
write_transients=32
reads=23496
read_transients=0
maintenance_operations=411
backups=35
writer_restarts=1
primary_bytes=1523712
backup_bytes=966656
status=ok
```

The 32 write transients occurred inside the deliberately marked writer-restart
window. Read conflicts caused by an active extension write transaction were
retried inside the libSQL reader adapter and did not escape into the soak.

An earlier run exposed `chunk row 87 read failed: Query returned no rows`.
The shared engine had published an intra-transaction flush location that only
the writer connection could see in its transaction-private shadow tables. A
new per-table read permit now prevents a writer from changing engine locations
while a reader materializes results; a reader arriving during another
connection's write transaction gets a retryable busy-style conflict instead of
following a private location. The writer connection retains read-your-writes.

This exact interleaving is pinned in extension unit tests, direct two-connection
CLI section 39 (rollback and commit), and the TimelessMetrics pooled-reader
regression. After the fix, the complete extension CLI suite, the mixed soak,
the Rust workspace, and all 477 Elixir tests pass.

## Decision and remaining gates

- Keep `:rust` as the default. Exact, narrow, wide, scalar, and exact-latest
  p95 all miss the fixed 20% bound.
- Keep `:libsql` opt-in. Its matcher pushdown, latest fan-out, window, batch
  ingest, storage, flush, rollup, migration, and direct-SQL capabilities are
  already compelling for suitable workloads.
- The stale-row soak failure is fixed and regression-tested; the mixed
  transaction/recovery gate is clean for this release candidate.
- The extension crate is pinned to merged `timeless-libsql` revision
  `09aa46e94185c8380d5a16a6efda353b62e0083a`; the unpacked Hex package builds
  the wrapper with `cargo check --locked` without the sibling checkout.
- Any eventual default change remains a separate, easy-to-revert commit.
