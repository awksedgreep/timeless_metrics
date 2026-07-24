# Plan: literal VictoriaMetrics drop-in parity for PromQL

**Date:** 2026-07-24
**Decision (Mark):** literal drop-in VM replacement is the guiding path.
Silent divergences get fixed immediately; rejected constructs get implemented
in priority order.
**Inputs:** notes/promql_conformance_audit_2026-07-24.md (§2 divergences,
§1 rejection tables), notes/bug_report_promql_compat_2026-07-24.md.
**Definition of done:** a query corpus (conformance battery + ddnet dashboard
queries) returns matching results from timeless-stack and a real
VictoriaMetrics instance, within float tolerance, timestamps exact; ddnet
dashboards run unmodified with the client-side workaround removed.

## Architecture decision: raw windowed evaluation

The seven divergences share one root: the PromQL evaluator delegates to
*forward-looking step-bucket aggregation*, while Prometheus/VM semantics are
*backward-looking windows evaluated on an exact grid*. Patching divergences
individually on top of bucketing cannot converge (e.g. `[5m]` windows, 5m
lookback fill, and reset-adjusted `rate` are unimplementable per-bucket).

Therefore the PromQL evaluator switches to **raw-sample windowed evaluation**:

- Primitive: `TimelessMetrics.query_multi(store, metric, labels, from:, to:)`
  already returns raw points per series on both engines — no native changes.
- For a range query `[start, end]` step `S`, fetch raw samples over
  `[start - max(window, lookback), end]`, then for each grid point
  `T = start + n*S`:
  - instant selector → last sample in `(T - lookback, T]` (VM's
    `default_rollup` behavior); no sample in lookback → gap (staleness).
  - rollup `fn(m[W])` → `fn` over samples in `(T - W, T]`, two-pointer
    sliding window, O(points + steps) per series.
- Output timestamps are exactly `start + n*S`. Values formatted as today.
- Lookback: default 300s, configurable (`:promql_lookback`); VM's
  auto-interval detection can come later — 5m matches Prometheus default and
  covers ddnet (60s scrape).
- The native params API (`metric=`, `group_by=`, TSBS paths) keeps bucketed
  pushdown untouched — zero risk to existing dashboards/benches. PromQL was
  the only consumer of the wrong semantics.
- Perf guardrail: `max samples per query` limit (Prometheus has the same
  concept); exceeding it → 422 `query would load N samples (limit M)`. Add a
  bench entry comparing evaluator throughput vs the old bucketed path so the
  regression is a measured, accepted cost. Optional later: native windowed
  rollup kernels in Rust (remember: bump version BEFORE native changes).

## Phase 0 — hygiene + gap radar (prereqs, small)

0.1 ~~Unify native bucket grid~~ **Superseded (Mark, 2026-07-24): retire the
    legacy engine instead of fixing it.** Done: `engine: :actor/:legacy/
    :sharded` logs a deprecation warning (removal in 7.0); all tests except
    text-series migrated to the Rust engine. The migration surfaced and fixed
    five Rust-path production bugs CI had never covered (it tested actor):
    - JSON `/api/v1/import` and the non-NIF Prometheus import path wrote
      points with **value and timestamp swapped** (data corruption).
    - Regex/negative label matchers crashed the NIF (`ArgumentError`) —
      and negative matchers (`!=`, `!~`) never worked on either engine;
      now a shared `TimelessMetrics.LabelMatch` with Prometheus
      missing-label-as-empty semantics serves both.
    - `latest`/`latest_multi`/`query_aggregate` routed to the legacy
      registry on Rust stores (crash).
    - No retention: nothing ever called `delete_before` on Rust stores —
      they grew unbounded. RustEngine now enforces schema raw retention
      hourly; `enforce_retention/1` works on both engines.
    - `rate` diverged between engines; unified as carry-in bucket rate in
      `Aggregation.bucket_rate/2` (one-sample-per-bucket data now yields
      rates; undefined-rate buckets omitted, matching Prometheus/VM).
    Also fixed: `transform=` was ignored on Rust query paths; points landing
    exactly on the range end were dropped by Rust bucketing.
    7.0 removal blockers: text series (Rust encode is numeric-only), rollup
    tiers/`query_daily`, true `mode: :memory` (livebooks use it; Rust engine
    silently writes to disk).
0.2 Accept RFC3339 `start`/`end`/`time` params (Prometheus API allows both;
    we only parse unix today).
0.3 **Gap radar:** count and sample rejected PromQL queries in Stats
    (construct name from the error, last N distinct query strings). Surfaced
    in /health/detailed. Real traffic then drives Phase 2/3 ordering instead
    of guesswork — this is how we avoid a third "missed it" event.
0.4 Fix dead `cross_series_aggregate` option in the native group_by HTTP path
    (it silently merges with the bucket aggregate today).

## Phase 1 — semantics core: clears ALL seven divergences (immediate)

1.1 Windowed evaluator per the architecture above. Rewires:
    - selectors (§2.1 bucket-avg → last-sample+lookback, §2.5 staleness fill)
    - `*_over_time`, honoring the range window (§2.2)
    - grid timestamps (§2.7 for the PromQL path)
1.2 Counter semantics (§2.3, §2.4) on raw windows:
    - `increase`: reset-adjusted sum of deltas over the window, using the
      sample just before the window for accuracy (VM-style, no Prometheus
      extrapolation guessing — VM deliberately dropped it; we follow VM since
      VM is the reference).
    - `rate` = increase / window_seconds. `irate` = last-two-samples slope
      with reset handling (stops being an alias).
    - Same math on both engines by construction (evaluator-level).
1.3 Duplicate/compound matchers (§2.6): selector matchers become a **list**
    `[{name, op, value}]`, not a map. Engine pre-filter uses the equality/
    regex subset; the evaluator applies the full AND list as a post-filter.
    This also unlocks the legitimate `{job=~"a.*", job!~"a-dev"}` pattern the
    map silently ate.
1.4 Update the three DIVERGENCE-pinned conformance tests to assert
    Prometheus/VM behavior; update audit doc §2 to "fixed in <version>".
1.5 **Differential harness** (the referee): `scripts/vm_diff/` or bench/ —
    starts a THROWAWAY VictoriaMetrics container (`podman run --rm` on a free
    port; never touches the quadlet services — those are Mark's production),
    seeds VM and a timeless instance with identical data via their import
    APIs, runs the corpus against both, diffs JSON (timestamps exact, values
    within 1e-9 relative). Runs on demand + before releases; conformance test
    tags `:vm_diff`, excluded unless VM is reachable.

## Phase 2 — PromQL breadth, by importance

Ranked by real-world Grafana/alerting usage; each is small once Phase 1's
windowed evaluator exists.

| # | Construct | Notes |
|---|---|---|
| 2.1 | vector matching `on()` / `ignoring()` / `group_left/right` | node_exporter & kube dashboards everywhere; ratio patterns |
| 2.2 | `histogram_quantile` (classic `_bucket`/`le`) | every latency panel; group by `le`, monotonic fixup, interpolate |
| 2.3 | `label_replace`, `label_join` | legend/relabel idioms; pure evaluator |
| 2.4 | gauge rollups `delta idelta deriv predict_linear changes resets` | `predict_linear` = disk-full alerts; least-squares over window |
| 2.5 | `quantile/stddev/stdvar/present_over_time` | trivial on raw windows |
| 2.6 | `absent`, `absent_over_time` | alerting staple; synthesize 1-series |
| 2.7 | `time timestamp scalar vector` | alert exprs (`time() - last_success`) |
| 2.8 | `sort sort_desc`, `count_values`, clock fns (`hour` …) | table panels, value histograms, silencing windows |
| 2.9 | math batch: `sgn`, trig family, `deg rad pi` | one sitting, ~30 lines |

## Phase 3 — MetricsQL tier (the "literal VM" part)

Full MetricsQL is 200+ functions; literal drop-in ≠ implement all of it.
Strategy: implement the constructs that appear in real VM dashboards and
ddnet-class clients; keep named "MetricsQL extension" errors for the tail;
let Phase 0's gap radar promote what actually gets queried.

Priority order:
3.1 `default` operator (`expr default 0`) — extremely common in VM dashboards.
3.2 `keep_metric_names` modifier.
3.3 `if` / `ifnot` operators.
3.4 `union()`, `alias()`, `label_set()`/`label_del()`.
3.5 `range_*` / `running_*` families, `quantiles()`.
3.6 Step-relative durations (`[5i]`) and MetricsQL's optional-window rollups
    (`rate(m)` without `[w]` — VM infers the window; our parser currently
    errors. With Phase 1 this becomes: window = max(step, lookback)).
3.7 Subqueries `m[5m:1m]` — inner grid eval + outer rollup; natural fit after
    Phase 1.
3.8 `@` modifier (trivial after grid evaluator).
3.9 `WITH` templates (parser-level; biggest single item — defer until radar
    shows demand).
Out of scope: Prometheus native histograms (VM doesn't support them either);
UTF-8 metric names until VM adopts them.

## Phase 4 — API-surface parity + release

4.1 `/api/v1/status/buildinfo` (Grafana uses it for flavor detection) +
    `/api/v1/status/config` stub.
4.2 `match[]` as repeated param everywhere (series/labels/label-values),
    `start`/`end` filters on labels endpoints, graceful `timeout=` accept.
4.3 Instant query: `resultType: vector` shape on `/api/v1/query` when
    `query=` present (today it reuses the range→instant conversion — verify
    exact VM shape in the differential harness).
4.4 Release train: mix.exs is 6.1.3 UNRELEASED → this lands as **6.2.0**
    (PromQL results change numerically = behavior fix, minor bump, changelog
    calls it out loudly). Then hex release, timeless-stack image bump,
    quadlet deploy, and acceptance: ddnet dashboards with
    `Poller.Metrics.VictoriaReader` retry-workaround removed.

## Verification stack (applies across phases)

1. `test/promql_conformance_test.exs` — classification fence, updated per
   phase (constructs move rejected → supported; divergence pins flip to
   parity assertions).
2. Differential harness vs real VM (Phase 1.5) — the ground truth for
   "literal".
3. Unit tests per construct with hand-computed expectations (existing style).
4. Bench guard for evaluator throughput + max-samples limit test.
5. Gap radar in production — post-deploy, the rejected-query counter should
   trend to zero on ddnet traffic.

## Risks / open items

- **Perf:** raw windowed eval reads more points than bucketed pushdown for
  wide ranges. Mitigations: max-samples cap, per-series streaming, later Rust
  kernels. Measure in Phase 1, don't guess.
- **VM behavioral trivia:** VM's rate/lookback edge cases are documented
  loosely; the differential harness decides disputes, not docs.
- **Matcher list change** touches selector plumbing (`selector_info`,
  series endpoint) — keep the map fast-path for pure-equality selectors.
- **Numeric output changes** in 6.2.0 will alter existing PromQL dashboard
  values (they were wrong per VM semantics, but visibly different post-
  upgrade). Changelog + stack release notes must state this.
