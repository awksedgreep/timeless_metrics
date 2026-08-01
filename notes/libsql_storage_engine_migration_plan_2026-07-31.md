# libSQL storage engine migration

This branch replaces the filesystem-backed Rust metrics store with the
`timeless-libsql` SQLite virtual table and shadow-block store in stages.

## Decisions

- Store compressed samples and administrative data together in `metrics.db`.
- Introduce `engine: :libsql` first; retain `engine: :rust` for rollback until
  parity, crash-safety, migration, and performance gates pass.
- Convert existing `rust_engine/` stores only through an explicit, verified
  offline migration. Never silently migrate during application startup.
- Keep PromQL semantics in Elixir. The storage boundary returns raw samples or
  explicitly requested mechanical reductions only.
- Defer the standalone Rust telemetry data-plane idea to a fresh post-migration
  branch. Its captured boundary and metrics-first POC are documented in
  [rust_telemetry_data_plane_poc_2026-07-31.md](rust_telemetry_data_plane_poc_2026-07-31.md);
  it does not change this branch's storage or query-performance scope.

## Sessions

1. [x] Expose the embedding and query-waist APIs from `timeless-libsql`.
2. [x] Package and load the extension from `timeless_metrics`; add a writer and
   reader connection pool over the existing `metrics.db`.
3. [x] Bind writes, resolved-series handles, flush, compaction, retention, and
   engine statistics.
4. [x] Bind raw reads and discovery while preserving the existing public result
   shapes and matcher semantics.
5. [x] Bind rollups, backup, health, HTTP ingest, and operational documentation.
6. [x] Add an offline importer which stages a new database, verifies every point,
   fingerprints its source, runs SQLite integrity checks, and activates only on
   explicit request.
7. [x] Complete the release validation and record the rollout decision: keep
   `:rust` as the default for the next release or two while `:libsql` ships as
   the fully supported forward path.

## Implementation status (2026-07-31)

Completed validation:

- `mix test`: all 477 tests pass, including dedicated libSQL engine,
  migration, backup, rollup, retention, and HTTP coverage.
- `cargo test --workspace`: all codec, core, extension, recovery, retention,
  rollup, query-kernel, and publication tests pass.
- `tests/cli.sh`: all 39 sections pass, including the 150k-operation oracle,
  five random `kill -9` recoveries, multi-connection visibility, resolved
  batches, `timeless_raw`, packed `timeless_raw_batches` reads, and the native
  scalar-aggregate SQL contract, newest-first latest-point contract, and
  transaction-safe catalog publication and invalidation. Section 39 pins the
  reader/writer interleaving that formerly exposed transaction-private chunk
  locations across connections.
- VictoriaMetrics differential run with `engine: :libsql`: all 182 range,
  instant, and metadata comparisons match.
- A Rust-engine fixture migrates in two steps, preserves empty catalog series,
  verifies float bits, refuses stale-source activation, retains rollback data,
  recovers from an injected activation-rename failure, and starts successfully
  as `engine: :libsql`.
- Same-host large-scale embedded benchmark (10K series, 1K population points,
  then 10-second steady phases): libSQL batch ingest reached 1.41M points/s
  versus Rust's 1.21M; compressed storage was 1.586 versus 2.350 bytes/point.
- Killing and supervising the Elixir libSQL writer preserves flushed blocks,
  drops only explicitly non-durable buffered points, and accepts new writes
  after restart.
- The first extension-first query optimization is complete: the 12K-series,
  720K-point scalar average fell from 449.270ms to 34.659-36.558ms through the
  public API (1.32-1.39x the latest Rust median), while direct SQL improved
  6.38-6.86x over raw materialization.
- Native latest is complete: the same 12K-series public query fell from the
  408-499ms baseline to 35.782-39.285ms and is 1.29-1.42x faster than the
  matching 50.708ms Rust median. Exact latest is 0.248ms versus Rust's 0.128ms
  (1.94x), and direct long-history SQL improved 14.89x.
- Native complete buckets are integrated through the public packed-window TVF.
  On 12K series and 72K output buckets, libSQL average fell from the 495.455ms
  raw baseline to 127.532ms median / 135.006ms p95 (3.88x), versus the matching
  Rust engine's 119.937ms / 124.265ms. Partial terminal buckets and Timeless
  rate carry-in have explicit raw-fallback tests; Victoria parity remains
  182/182.
- Wide raw transport now uses the additive one-row `timeless_raw_frame` TVF.
  The 12K-series/720K-point eager public read fell from the Session 5 starting
  median of 344.221ms to 113.142ms / 113.881ms p95 (3.04x), while exact and
  narrow reads improved. Native decoding constructs final series maps directly;
  sampled peak memory is 9.306x the serialized result under an enforced 10x
  benchmark bound. The original per-series TVF remains public for compatibility.
- Catalog publication no longer makes the first reader reload state already
  present in the shared engine. The direct first exact read after flush fell
  from 44.730ms to a 0.187ms fresh-process median (239x), while the public
  TimelessMetrics path fell from 54.818ms to 2.891ms and clears its 5ms gate.
  Commit, rollback, compaction, prune, two-connection, external-process, and
  reopen behavior are pinned by core and CLI tests.
- Hybrid matcher planning now pushes portable regex, negative, and
  empty-equality predicates below the storage boundary while retaining exact
  Elixir residuals for duplicate or dialect-sensitive cases. A selective
  one-series regex read fell from 116.944ms to 3.083ms (37.93x), and filtered
  discovery fell from 52.427ms to 2.431ms (21.57x). Direct SQLite users gain
  matcher-aware `timeless_series` and `timeless_label_values` forms as well.
- A mixed 30-second run with eight readers completed 392,512 writes, 23,496
  oracle reads, 411 maintenance operations, 35 online backups, a forced writer
  restart, reopen, and backup verification with zero escaped read transients.
  Extension read permits plus adapter retries close the transaction-private
  chunk visibility failure found by the earlier soak.
- The packaged native wrapper is pinned to merged `timeless-libsql` revision
  `09aa46e94185c8380d5a16a6efda353b62e0083a`. A clean Hex package unpack and
  `cargo check --locked` compile without the sibling checkout, and all 477
  Elixir tests pass against that Git dependency.

Release follow-up:

- Extend activation fault injection to every filesystem rename before removing
  the migration preview label; the critical staged-database rename, writer
  process, and upstream SQLite crash/restart paths are covered.
- Exact, narrow, wide, scalar, and exact-latest p95 differences are recorded
  rather than treated as migration blockers. Continue optimizing them when the
  profile identifies worthwhile extension-first work.
- Change the default from `:rust` to `:libsql` in a separate, easy-to-revert
  release after the opt-in engine has had one or two releases of field use.

## Acceptance gates

- Full Elixir and Rust suites pass, including restart, concurrency, backup,
  retention, rollup, malformed-input, and migration fault cases.
- The complete VictoriaMetrics differential corpus retains parity.
- On the same host, report sustained ingest, query-p95 distributions, and
  storage size without hiding regressions; the recorded product decision, not
  a synthetic relative threshold, controls the default-engine rollout.
- A converted store can use `engine: :rust` again without deleting its original
  `rust_engine/` rollback data.
