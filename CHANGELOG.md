# Changelog

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
