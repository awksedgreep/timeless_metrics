# Metrics API POC Session 6: scheduling, maintenance, and final verdict

Date: 2026-08-01  
Branches: `poc/rust-telemetry-data-plane` in `timeless-libsql`,
`timeless_metrics`, and `timeless_ui`

## Outcome

Keep the Rust metrics data plane and retain two SQLite reader connections as
the default. The completed POC preserves the existing `timeless_metrics`
extension as the only storage/query engine, removes the BEAM/NIF boundary from
the implemented HTTP slice, and leaves Phoenix/LiveView as the product and
cluster control plane.

No admission controller, cross-request transaction grouping, or API-only
maintenance policy was added. The measured admission lock is not a bottleneck,
the ordered writer drains exactly, and changing the established flush cadence
would change durability behavior. Maintenance improvements should be new
public extension policy that also benefits direct SQLite/libSQL users.

## Pinned workload

All concurrency and load-shape runs used the same release artifacts and fresh
database per row:

- 200 devices x 20 metrics = 4,000 series;
- four concurrent writers, 50 devices/request = 1,000 points/request;
- six fixed three-second steps at 100, 50, 25, 12.5, 6.25, and 3.125ms;
- one-second warmup and one-second settle, seed 42001;
- Prometheus text ingest through the extension's fused public batch surface;
- native latest/range query mix; and
- one ordered SQLite writer, explicit final flush, and drain to zero.

The reader sweep deferred scheduled maintenance so only reader topology
changed. Linux `VmHWM` was read from `/proc/<pid>/status` before graceful
shutdown.

## Reader sweep

| readers | completed points/s | write p95 | query p95 | query p99 | final queue age | drain | HWM |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 854.8K | 433us | 10.51ms | 23.57ms | 8ms | 816.64ms | 171,192KiB |
| **2** | **869.9K** | **413us** | **8.52ms** | **14.35ms** | **8ms** | **704.32ms** | **181,080KiB** |
| 4 | 866.9K | 408us | 7.46ms | 12.32ms | 12ms | 727.34ms | 192,080KiB |
| 8 | 854.9K | 434us | 11.28ms | 36.23ms | 8ms | 804.04ms | 194,592KiB |

Every run returned 4,000 series, completed all admitted points, reported zero
read/write errors, and finished at queue/in-flight `0/0`. Four readers buy only
1.06ms at saturated p95 while adding 10.7MiB over two readers; eight readers
regress. Two is the best overall operating point, not a value inherited from
the logs POC.

The chosen two-reader run admitted 7,851 batches and 7.851 million points.
Admission-lock wait totaled 2.79ms, or 0.36us/request. Maximum writer queue
wait was 10.60ms. That evidence rejects an admission-fairness layer and
cross-request transaction grouping for this POC.

## Query-worker isolation

| query workers | completed points/s | write p95 | query p95 | query p99 | drain | HWM |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 857.9K | 428us | - | - | 793.55ms | 180,312KiB |
| 1 | 859.2K | 426us | 8.93ms | 15.63ms | 803.50ms | 180,960KiB |
| 2 | 869.9K | 413us | 8.52ms | 14.35ms | 704.32ms | 181,080KiB |

The small ordering differences are client/host noise: zero, one, and two query
workers occupy the same write-throughput band. Two query workers therefore do
not consume meaningful ingest capacity, and peak memory changes by less than
0.8MiB across the matrix.

## Final same-host product context

The stronger of the two maintenance-deferred Elixir+libSQL controls from
Session 3 and the established Rust-block product reference from Session 0 put
the final mixed result in context. The Rust block row is a secondary product
comparison, not the API-boundary control.

| process / storage | completed points/s | write p95 | query p95 | query p99 | HWM |
|---|---:|---:|---:|---:|---:|
| Elixir API + libSQL | 782.5K | 912us | 70.26ms | 134.28ms | 460,152KiB |
| **Rust API + libSQL, final** | **869.9K** | **413us** | **8.52ms** | **14.35ms** | **181,080KiB** |
| Elixir API + Rust block | 806.6K | 765us | **1.22ms** | **1.73ms** | 290,452KiB |

Against the stronger process-boundary control, the final Rust API completes
11.2% more points, has 8.25x lower query p95, and uses 60.7% less peak resident
memory. The mature block engine remains 6.98x faster at query p95, but the Rust
API + libSQL path completes 7.8% more writes in this client-limited run and
uses 37.7% less peak memory. The remaining read gap is accepted; it does not
invalidate the SQLite/libSQL product direction.

## Every included read shape

A fresh two-reader process was seeded with exactly 4,000 series and 400,000
points at fixed timestamp 1785600000, flushed, and measured sequentially with
scheduled maintenance deferred.

| shape | p50 | p95 | p99 | response bytes |
|---|---:|---:|---:|---:|
| exact latest | 272us | 620us | 674us | 103 |
| exact 15s range | 309us | 538us | 627us | 283 |
| exact raw export | 528us | 723us | 785us | 2,007 |
| all label names | 8.16ms | 9.57ms | 10.42ms | 62 |
| metric label values | 460us | 678us | 702us | 3,229 |
| metric series | 602us | 820us | 859us | 13,979 |
| exact selector series | 646us | 831us | 857us | 8,129 |
| selector exact range | 335us | 634us | 706us | 2,155 |
| selector 100-series range | 2.00ms | 2.73ms | 2.83ms | 224,058 |
| `avg_over_time` exact range | 303us | 630us | 719us | 3,028 |
| `avg_over_time` 100-series range | 2.23ms | 2.98ms | 3.13ms | 302,624 |
| selector exact instant | 551us | 767us | 965us | 182 |
| `avg_over_time` exact instant | 378us | 615us | 675us | 183 |

The read-shape process peaked at 55,996KiB. All shapes retained their pinned
response sizes and the extension-backed contract tests provide exact semantic
parity, malformed-input, cancellation, flush, and reopen coverage.

## Maintenance under load

The real defaults are a ten-second flush, five-minute compact/rollup, and
hourly retention pass. With defaults enabled, the same mixed workload's final
step completed 767.1K points/s with 10.31ms query p95, 30.61ms p99, zero
errors, and 186,572KiB HWM. Periodic flushes accumulated 998ms of SQLite work
over the observed server lifecycle and created 16,000 partial raw chunks where
the maintenance-deferred run had 4,000 natural chunks. Maximum writer queue
wait was 629ms. This is a real flush/durability tax and fragmentation signal,
not an HTTP admission failure.

An accelerated diagnostic used ten-second flush, five-second compact, and
seven-second retention intervals. Across the complete observed lifecycle it
ran seven flushes (1.19s total), fourteen compactions (4.15s), and ten prunes
(5.92ms). It remained exact, error-free, and drained to zero, but maximum
writer queue wait reached 1.24s, query p99 reached 106.77ms, and HWM reached
208,052KiB. Prune is negligible when no data expires; compact/flush are the
serialized work. These deliberately accelerated figures are diagnostic and
must not be quoted as the production cadence.

Changing the ten-second durability window only for this API would violate the
POC boundary. The reusable follow-up is an extension-level age/occupancy-aware
flush policy, measured separately for low-volume durability and high-volume
chunk quality. It is not required to keep this process boundary.

## Physical high-water, reuse, and explicit vacuum

Repeated accelerated compaction produced a 27,492,352-byte checkpointed
database: 1,678 SQLite pages, of which 823 pages (13,484,032 bytes) were on the
freelist. Logical metrics payload remained 10,645,140 bytes. These values are
reported separately; logical compression is not physical file size.

Two copies established the two distinct behaviors required by the plan:

1. Reopening one copy and adding four million points plus 4,000 chunks left
   the database high-water exactly 27,492,352 bytes. Freelist pages fell from
   823 to 764, proving SQLite reused free pages before growing the file.
   Graceful shutdown checkpointed and removed the WAL without changing the
   database size.
2. Running an explicit offline `VACUUM` on the other copy reduced it from
   1,678 pages / 823 free to 849 pages / zero free: 27,492,352 bytes became
   13,910,016 bytes.

No automatic-vacuum policy was added. Reuse is automatic SQLite behavior;
returning the high-water to the filesystem is an explicit operational choice.

## Final compatibility and isolation gates

The final gate runs:

```text
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --manifest-path poc/timeless-metrics-api/Cargo.toml \
  --all-targets -- -D warnings
cargo test --manifest-path poc/timeless-metrics-api/Cargo.toml
TIMELESS_EXT_PATH=$PWD/target/release/libtimeless_ext.so \
  cargo test --manifest-path poc/timeless-metrics-api/Cargo.toml \
  --test storage_contract -- --ignored
./tests/cli.sh
MIX_ENV=test mix test
mix test test/timeless_ui/metrics_data_plane/client_test.exs \
  test/timeless_ui/metrics_data_plane/canvas_source_test.exs \
  test/metrics_data_plane_integration_test.exs
```

`tests/cli.sh` contains the extension SQL/CLI coverage plus the three-seed,
50,000-operation plain-table oracle and five-iteration kill-9 crash gate. The
UI integration gate includes second-owner rejection, forced child kill/restart,
exact reopen results, incomplete-response rejection, graceful SIGTERM flush,
and child reaping.

Final results:

| gate | result |
|---|---:|
| Rust workspace | 169 passed |
| metrics API unit tests | 16 passed |
| metrics API real-extension contracts | 5 passed |
| extension SQL/CLI suite | all 43 sections passed |
| randomized oracle | 3 x 50,000 operations passed |
| kill-9 recovery | 5/5 iterations passed |
| `timeless_metrics` | 482 passed |
| focused UI boundary | 8 passed |
| full `timeless_ui` | 77 passed |

Rustfmt, strict metrics-API Clippy, and Elixir formatting checks also passed.

## Verdict and next signal boundary

Session 6 meets the final exit criterion. The Rust boundary is exact for its
declared API slice, materially smaller than the Elixir data plane, keeps read
tails bounded under realistic writes, preserves the extension for direct
SQLite/libSQL users, and has explicit crash/restart and physical-storage
behavior. Keep it.

The next POC should be traces. Reuse the one-writer/bounded-reader topology,
completion-aware health/flush accounting, cancellation, direct response
encoding, and OTP process supervision. Begin from the existing traces virtual
table and public query surfaces, baseline its real API contract first, and add
missing reusable query primitives to the extension. Do not create a generic
three-signal server until traces identifies which code is genuinely common.
