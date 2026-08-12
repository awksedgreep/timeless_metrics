# Changelog
## 6.6.1 (2026-08-11)

**Vendored SQLite extension moves to timeless-libsql v0.6.2.** Each optimize
pass now persists cumulative raw-in/compressed-out byte totals in the store's
`_meta`, exported via `timeless_stats` as `compression_input_bytes_total` /
`compression_output_bytes_total` — durable across restarts, unlike the
process-local optimize profile counters. Pairs with timeless_logs /
timeless_traces 1.11, whose dashboards show a compression ratio backed by
these totals.

## 6.6.0 (2026-08-11)

**Vendored SQLite extension moves to timeless-libsql v0.6.1.** Two behavior
changes ride the bundle:

- *Auto-optimize:* the logs and spans engines run a budgeted optimize from
  the flush path (every 30th flush, or immediately at 32k raw entries), so
  embedded stores compress without any host scheduling. Previously a host
  that only flushed accumulated raw blocks until retention deleted them
  uncompressed.
- *Engine pins:* the engine and its buffer survive writer-connection churn
  in the extension's process-global registry — a killed writer process no
  longer loses buffered points; the durability loss boundary is OS-process
  death.

Pairs with timeless_logs/timeless_traces 1.10 (compressed stats surfaced)
and the 0.7.18/0.3.16 dashboards (tiles read them).

## 6.4.2 (2026-08-10)

**Vendored SQLite extension moves to timeless-libsql v0.6.0.** That release adds
`reindex:<keys>`, which rewrites a store's posting lists so a widened
`index_keys` allowlist applies to blocks written before the change. `index_keys`
is fixed at virtual-table creation and postings are written at insert time, so
without it a newly indexed key prunes away every older block — the entries are
still stored, but queries stop returning them.

No behaviour change for metrics; this is the extension `timeless_logs` needs in
order to migrate an existing store.

## 6.4.1 (2026-08-09)

**The rust_engine conversion no longer holds whole series in memory.** It
materialised each series as an Elixir list several times over, so peak memory
tracked the largest series rather than any batch size. On a 3.9 GB host this
OOM-killed the VM mid-migration and left a staged store the restart could not
resume.

Points are now read a page at a time through the legacy reader's cursor,
written as each page arrives, and folded into a running digest. Verification
reads the target back in density-sized timestamp windows and compares count
and digest, instead of holding source and target in memory together.

Paging alone made the conversion quadratic in time, which turned out to be
three defects in `legacy_query_page`, all on the read side. It never pruned
chunks, passing the full int64 range even though every chunk carries
`min_ts`/`max_ts` and the cursor sets a floor. It re-decompressed each chunk
on every page. And it built a full sort key, including a `String` clone, for
every point of every page before discarding nearly all of them. The third
dominated; the first two together were worth about 25%.

Measured over one series, peak memory and wall clock:

| points | before | after |
|---|---|---|
| 1M | 628.5 MB / 1166 ms | 32.6 MB / 1081 ms |
| 2M | 1101.4 MB / 2152 ms | 29.3 MB / 2326 ms |
| 4M | 2353.2 MB / 4094 ms | 29.2 MB / 5335 ms |

Memory is flat where it grew linearly — 80x lower at 4M points — and wall
clock is comparable to the original rather than 5x worse. Time is still mildly
superlinear because each page rescans its chunk, now at two integer
comparisons per point.

No public API change.

## 6.4.0 (2026-08-09)

**Automatic rust_engine conversion.** Starting a store on the (default)
libSQL engine over an unmigrated legacy `rust_engine/` directory now
converts it automatically at startup — the same staged, journaled,
digest-verified migration as `mix timeless_metrics.migrate_libsql
--activate`, invoked for you: startup blocks until the conversion
verifies and activates, the legacy store and pre-migration database are
preserved for rollback, and restarts never re-convert. Set
`auto_migrate: false` to restore the strict refusal. The legacy `:rust`
engine is deprecated for removal in roughly three months (~2026-11).

Also fixes single-shot `--activate` activation: the staged candidate is
checkpointed before the rename (previously its live WAL could strand
commits and the leftover sidecars failed the stage-dir cleanup).

## 6.3.0 (2026-08-08)

**libSQL is now the default storage engine** — the final step of the
libSQL migration plan, after its opt-in field period. `engine: :rust`
remains available as the explicit rollback configuration; startup still
refuses loudly on an unmigrated non-empty `rust_engine/` directory (run
`mix timeless_metrics.migrate_libsql` first).

- Re-pinned the embedded `timeless-libsql` extension from a pre-handshake
  Aug 1 development rev to the released **v0.5.0** tag (CI now checks out
  the same tag). Stored data needs no migration: data ABI stays 1 and the
  packed frame formats are unchanged.
- The libSQL writer now performs the capability preflight at startup
  (`timeless_capabilities()`: data ABI 1 + resolved-v1 batches), logs the
  negotiated extension version, and refuses a pre-handshake extension
  with an error naming the requirement.
- The writer traps exits so its final ingest-transaction commit + flush
  run on ordinary supervisor shutdown; buffered points survive restarts.
- Reads cooperate with the v0.5.0 extension's publication gate: a
  "retry, as for SQLITE_BUSY" gate error re-barriers and retries a
  bounded number of times instead of surfacing on the first attempt.

## 6.2.6 (2026-08-03)

Merged the Rust telemetry data-plane release: the external
`timeless-metrics-api` owner becomes the production Stack default, with
this OTP application loaded for compatibility and migration
(`owner: :external` starts no second storage owner). CI builds
`timeless-libsql` from source and runs the suite against it.

## 6.2.5 (2026-08-03)

Packaging only: fixed arm64 macOS zstd cross-compilation for the
precompiled NIF artifacts.

## 6.2.4 (2026-08-03)

Packaging only: fixed macOS precompiler tap trust for published
artifacts.

## 6.2.3 (2026-08-03)

The libSQL storage engine lands as an **opt-in** (`engine: :libsql`):
the `timeless-libsql` SQLite extension stores blocks, rollups, the
series registry, and admin data in one `metrics.db`, consuming the
packed query-frame surfaces (TRF1/TAF1/TLF1/TRB1). Recorded validation:
1.41M pts/s ingest, 1.586 bytes/point, VM differential 182/182, and
frame-path query wins up to 37–239x.

- Verified offline Rust→libSQL conversion (`mix
  timeless_metrics.migrate_libsql`), bounded resumable release
  migration with journaling and identity digests, and a crash-safe
  release-startup cutover state machine for the external stack.
- External storage ownership mode (`owner: :external`).
- Metrics API POC sessions 1–6 recorded; the standalone Rust data plane
  kept for the release that became 6.2.6.

## 6.2.2 (2026-07-24)

Performance: PromQL evaluation parallelizes per series across schedulers,
and the redundant re-sort of engine-sorted points is gone. No semantic
changes — 182/182 VM differential parity retained. On a 200-series /
1.15M-point / 24h workload: `sum by (host) (rate(m[5m]))` 423ms → 188ms,
`rate` 371ms → 244ms, and wide-window rollups improved ~10x
(`max_over_time(m[6h])` at 60s step: 2.7s → 254ms). Baseline and floor
analysis in `bench/results/2026-07-24_promql_windowed_eval.md`.

## 6.2.1 (2026-07-24)

Completes the Prometheus/VM API-compat surface started in 6.2.0. The
differential referee now also covers instant queries and metadata
endpoints — 182/182 corpus parity against live VictoriaMetrics across
`query_range`, `query`, `labels`, `label values`, and `series`.

- `/api/v1/labels` and `/api/v1/label/:name/values` (and `/prometheus`
  mirrors) honor repeated `match[]` selectors, restricting results to
  matching series. `start`/`end` are accepted (not applied — the label
  index is not time-partitioned).
- Native `/api/v1/series` accepts `match[]` selectors alongside its
  `metric=` form and unions repeated matchers.
- Instant-query (`/api/v1/query`) vector responses verified against VM
  for selectors, rollups, aggregations, arithmetic, `time()`,
  `vector()`, `absent()`, and empty results — no changes needed.

## 6.2.0 (2026-07-24)

The VictoriaMetrics-parity release. The PromQL engine was rewritten from
scratch and every supported construct is now verified against a live
VictoriaMetrics instance by a differential harness
(`scripts/vm_diff.exs`, 158-query corpus, exact timestamps, 1e-6 value
tolerance). Full background: `notes/promql_conformance_audit_2026-07-24.md`
and `notes/promql_vm_parity_plan_2026-07-24.md`.

### ⚠️ Behavior changes (intentional)

- **PromQL results change numerically.** The old evaluator returned
  forward-looking bucket averages; the new one implements Prometheus/VM
  semantics — last-sample-within-lookback selectors on an exact
  `start + n*step` grid, honored range windows, reset-adjusted
  `rate`/`increase`, `irate` as last-two-samples. Dashboards will show
  (correct) different values after upgrading.
- Unsupported PromQL now returns Prometheus-style
  `{"status":"error", ...}` (HTTP 400/422) instead of silent
  empty-success responses.
- Rust-engine stores now enforce schema raw retention hourly. Stores
  that relied on the previous (absent) retention will begin deleting
  expired data.
- `rate`-family bucket aggregation (native `aggregate=rate` API) uses
  carry-in bucket rates on both engines; buckets with no computable
  delta are omitted instead of filled with zero.
- NaN samples are omitted from PromQL query output (VM behavior).

### Fixed (production bugs found by the engine-retirement test migration)

- **Data corruption:** JSON `/api/v1/import` and the non-NIF Prometheus
  import path wrote every point with value and timestamp swapped on
  Rust-engine stores.
- Regex label matchers crashed the Rust NIF; negative matchers
  (`!=`, `!~`) never matched anything on either engine. A shared
  `TimelessMetrics.LabelMatch` implements all four matcher forms with
  Prometheus missing-label semantics.
- `latest/3`, `latest_multi/2,3`, and `query_aggregate/4` crashed on
  Rust-engine stores (legacy-registry routing).
- No retention was ever enforced on Rust-engine stores (unbounded
  growth).
- `transform=` was ignored on Rust query paths; samples landing exactly
  on the range end were dropped.
- `cross_series_aggregate` was silently ignored by grouped queries.
- RFC3339 `start`/`end`/`time` parameters parsed as year-as-seconds.
- Chunk-index shadowing when two chunks share `(series_id, min_ts)`
  (unreleased 6.1.3 fix, included here).

### Added — PromQL engine

- Real tokenizer + recursive-descent parser + windowed evaluator
  covering effectively the full PromQL surface: all matcher types
  (duplicates AND), `offset`, `@`, subqueries `[W:R]`, step-relative
  `[Ni]` windows; the complete rollup family incl. `delta idelta deriv
  predict_linear changes resets quantile/stddev/stdvar/present_over_time`;
  aggregations incl. `count_values`, prefix/suffix `by`/`without`;
  vector matching (`on`/`ignoring`/`group_left`/`group_right`);
  `histogram_quantile`; `label_replace`/`label_join`;
  `absent`/`absent_over_time`; `time`/`timestamp`/`scalar`/`vector`;
  clock functions; `sort`/`sort_desc`; `sgn`, trig, `deg`/`rad`/`pi`.
- MetricsQL tier: `default`/`if`/`ifnot` operators,
  `keep_metric_names`, `union`, `alias`, `label_set`, `label_del`,
  `default_rollup`, `range_*`, `running_*`, window-less rollups.
- Referee-verified VM behaviors: per-function `__name__` retention,
  implicit-zero series heads with the rollupDelta magnitude heuristic,
  duplicate-output-timeseries errors on label transforms.
- Guards: 11k points-per-series resolution cap and a raw-sample budget
  (`config :timeless_metrics, promql_max_samples:`), lookback via
  `promql_lookback_seconds` (default 300).

### Added — HTTP API

- POST support on `/api/v1/query`, `/api/v1/query_range`, and the
  `/prometheus` mirrors (form bodies).
- `/api/v1/status/buildinfo` (+ mirror) and `/api/v1/status/config`
  for Grafana flavor detection.
- Repeated `match[]` parameters on the series endpoint (union).
- Gap radar: rejected PromQL queries are counted and sampled in
  `/health/detailed` (`promql_rejected`, `promql_rejections`) so real
  traffic ranks what to implement next.

### Deprecated

- The legacy Elixir engine (`engine: :actor`/`:legacy`/`:sharded`) logs
  a deprecation warning; removal planned for 7.0. Remaining
  legacy-only features: text series, rollup tiers/`query_daily`, true
  `mode: :memory`.

## 6.1.2 (2026-07-18)

- Raw-first compaction (5.5x flush, -27% storage, narrow queries -56%),
  crash-safe via compaction manifest. Library default off;
  timeless-stack enables it.

Earlier releases: see git history.
