# PromQL conformance audit

**Date:** 2026-07-24
**Scope:** `TimelessMetrics.PromQL` (post-rewrite, same day) vs the Prometheus 3.x
query language reference, plus commonly-hit VictoriaMetrics MetricsQL extensions.
**Method:** every construct below was executed against a seeded store via
`parse/1` + `execute/5` (battery script), plus targeted experiments for semantic
divergences. Nothing in this doc is classified from reading docs alone.
**Regression fence:** `test/promql_conformance_test.exs` pins every
classification — a construct moving between classes fails CI.

Classes:

- ✅ **Supported** — parses and executes with Prometheus-equivalent results
- ⚠️ **Different** — works, but semantics diverge from Prometheus (the dangerous
  class; each divergence detailed in §2)
- ❌ **Rejected** — returns `{"status":"error"}` with a named message
  (never empty-success)

## 1. Language surface

### Selectors and matchers

| Construct | Status |
|---|---|
| `m`, `m{}`, `m{a="x"}`, `!=`, `=~`, `!~` | ✅ |
| `{__name__=~"re"}` multi-metric | ✅ |
| `{a="x"}` nameless (all metrics) | ✅ |
| Duplicate matchers `m{a="x",a="y"}` | ⚠️ last matcher wins; Prometheus ANDs all matchers (§2.6) |
| UTF-8 quoted metric names `{"my metric"}` | ❌ |
| Negative `__name__` matchers | ❌ |

### Modifiers

| Construct | Status |
|---|---|
| `[5m]`, compound `[1h30m]` | ⚠️ parses fully; window is **ignored** — bucket = step (§2.2) |
| `offset 5m`, `offset -5m` | ✅ (negative offset allowed without a feature flag) |
| `@ <ts>`, `@ start()`, `@ end()` | ❌ |
| Subqueries `m[5m:1m]`, `(expr)[5m:]` | ❌ |

### Binary operators

| Construct | Status |
|---|---|
| `+ - * / % ^` vector∘scalar, scalar∘vector | ✅ (drops `__name__`, `/0` → `+Inf`/`-Inf`/`NaN`) |
| `+ - * / % ^` vector∘vector 1:1 exact-label match | ✅ (many-to-many → clear error) |
| Comparisons `== != > < >= <=` (filtering) | ✅ per-sample filtering, on bucketed values (§2.1) |
| `bool` modifier | ✅ (and scalar-scalar comparison without `bool` errors, as Prometheus) |
| `and` / `or` / `unless` | ✅ per-signature + per-timestamp |
| `on()` / `ignoring()` / `group_left` / `group_right` | ❌ |
| Unary minus incl. `-2^2 = -4` precedence | ✅ |

### Aggregation operators

| Construct | Status |
|---|---|
| `sum avg min max count group stddev stdvar` | ✅ |
| `topk(k,…)` `bottomk(k,…)` `quantile(q,…)` | ✅ (per-step ranking; quantile linear-interpolated like Prometheus) |
| `by (…)` / `without (…)`, prefix and suffix, `by ()` | ✅ (bare form collapses to one series, drops `__name__`) |
| `count_values` | ❌ |
| `limitk` / `limit_ratio` (3.x experimental) | ❌ |

### Range-vector functions (rollups)

| Construct | Status |
|---|---|
| `avg/min/max/sum/count_over_time` | ⚠️ window ignored — computed per step-bucket (§2.2) |
| `last_over_time`, `first_over_time` | ⚠️ same; keeps `__name__` (correct) |
| `rate` | ⚠️ window ignored; counter resets *skipped* not reset-adjusted; implementation differs between actor and Rust engines (§2.3) |
| `irate` | ⚠️ alias of `rate` — not last-two-samples (§2.3) |
| `increase` | ⚠️ = `rate × step`, not extrapolated increase over the window (§2.4) |
| `delta idelta deriv predict_linear resets changes` | ❌ |
| `quantile/stddev/stdvar/mad/present_over_time` | ❌ |
| `double_exponential_smoothing` (`holt_winters`) | ❌ |

### Value transforms

| Construct | Status |
|---|---|
| `abs ceil floor round round(v,n) sqrt exp ln log2 log10` | ✅ (drop `__name__`; `ln(0)` → `-Inf`, `ln(<0)` → `NaN`) |
| `clamp clamp_min clamp_max` | ✅ |
| `sgn`, trig (`sin cos tan …h, a…`), `deg rad pi` | ❌ (trivial to add — see §3) |

### Label manipulation, sorting, existence

| Construct | Status |
|---|---|
| `label_replace label_join` | ❌ |
| `sort sort_desc sort_by_label sort_by_label_desc` | ❌ (matrix results are label-sorted for determinism) |
| `absent absent_over_time` | ❌ |

### Histograms

All ❌: `histogram_quantile histogram_fraction histogram_avg histogram_count
histogram_sum histogram_stddev histogram_stdvar`. No native-histogram sample
type exists in the store; classic `_bucket`/`le` quantiles are implementable
in the evaluator (§3 P1).

### Scalar/time functions

All ❌: `scalar vector time timestamp minute hour day_of_* days_in_month month
year info pi`.

### VictoriaMetrics MetricsQL extensions

All ❌ with a distinct message naming them as MetricsQL: functions
(`default_rollup label_set alias union range_* running_* quantiles distinct
keep_last_value …`, `WITH` templates) and operators (`default`, `if`, `ifnot`,
`keep_metric_names`). Relevant because the container advertises VM
compatibility on :8428 — a VM-tutorial query now says *why* it fails instead
of returning nothing.

## 2. Semantic divergences (work, but differ from Prometheus)

> **STATUS UPDATE (2026-07-24, Phase 1):** all seven divergences below are
> **fixed** by the windowed evaluator (see the parity plan, Phase 1, and the
> PARITY tests in test/promql_conformance_test.exs). §2.1/§2.5: selectors now
> evaluate last-sample-within-lookback with staleness fill; §2.2: range
> windows are honored; §2.3/§2.4: rate/increase are reset-adjusted over the
> window with carry-in, irate is last-two-samples; §2.6: matchers are ANDed
> lists; §2.7: PromQL output timestamps are exactly `start + n*step` (the
> engine-grid mismatch became moot when the legacy engine was deprecated).
> The text below documents the pre-Phase-1 behavior for the record.
>
> **Referee finding (scripts/vm_diff.exs, 67/67 corpus parity against a real
> VM container):** VictoriaMetrics *keeps* `__name__` on `avg/min/max/last/
> first_over_time`, `ceil`, `floor`, `round`, and `clamp*` — strict
> Prometheus drops it there. We follow VM (the compatibility target). VM
> drops the name on `abs`, `sqrt`, `exp`, `ln`, `log*`, `sum/count_over_time`
> and all counter functions. VM also emits `increase = 0` for single-sample
> windows and, at a series head with no carry-in sample, divides `rate` by
> the actual data span inside the window rather than the window length —
> both matched.

These are the constructs a dashboard author would never notice from an error —
each verified with a concrete experiment.

### 2.1 Instant selectors return bucket aggregates, not last-sample-at-step

Prometheus evaluates `m` at step timestamp T as "most recent raw sample ≤ T
(within 5m lookback)". We return the **average of samples in the forward
bucket [T, T+step)**. With a gauge ramping 0→290 and step=300:
ours `[5.0, 40.0, 85.0…]` (bucket means), Prometheus `[0, 50, 100…]` (point
samples). Consequences: smoother charts, stat panels show bucket-mean not
latest, and values differ from VM for anything but constant series. This is a
deliberate TSDB design (bucketed pushdown) but it is **undocumented API
behavior** today.

### 2.2 Range windows are parsed and ignored

`avg_over_time(m[1m])` ≡ `avg_over_time(m[30m])` at the same step — verified
byte-identical output. The effective window is always the step. Same for
`rate`/`increase`. Grafana queries typically set range ≈ 2-4× scrape interval
with step ≥ range, so results are often *similar*, but `max_over_time(m[1d])`
with a 1m step does not do what it says at all.

### 2.3 rate/irate

- Counter resets: Prometheus adds the post-reset value (counter restarted at
  0); we **skip** the negative-delta interval → undercount in the reset bucket.
- `irate` is a straight alias of `rate` (Prometheus: slope of last two
  samples — spikier by design).
- The two engines compute differently: actor engine = pairwise slopes within
  the bucket; Rust engine = last-value delta between adjacent buckets ÷ step.
  Same query, same data, different engines → different numbers at bucket edges.

### 2.4 increase = rate × step

`increase(ctr[5m])` at step 600 returned 1000 (= 1.667/s × 600s), where
Prometheus returns ~500 (extrapolated growth over the *5m window*) regardless
of step. Unit dashboards ("requests in the last 5m") read 2× high here.

### 2.5 No lookback/staleness fill

Sparse series (sample every 120s) queried at step 30 returns 6 points where
Prometheus returns 21 (each step filled from ≤5m-old samples). Line charts
mostly hide this; single-stat panels querying tight windows can come up empty
where VM showed a value. (Prometheus staleness markers are likewise absent —
we can't distinguish "series went away" from "no sample this bucket".)

### 2.6 Duplicate matchers: last one wins

`m{h="nope",h="x"}` returns the `h="x"` series (matchers stored in a map);
Prometheus ANDs all matchers → empty. Nobody writes this on purpose, but
generated queries do.

### 2.7 Output timestamp grid depends on engine

Actor engine buckets are **epoch-aligned** (`floor(ts/step)*step` — a query
starting at :00:20 with step 300 returns timestamps at :58:00, :03:00…);
Rust engine aligns buckets to the requested `start`. Prometheus returns
exactly `start + n*step`. Grafana pre-aligns start to step so this is usually
invisible, but ad-hoc API consumers see off-grid timestamps, and the two
engines disagree with each other.

## 3. Prioritized roadmap

**P0 — decide & document semantics (no code until decided):**
1. Bucket-aggregate vs last-sample selector semantics (§2.1) and lookback fill
   (§2.5). Options: (a) document as intended TSDB behavior, (b) add a
   `last`-aggregate + carry-forward mode for PromQL paths to match
   Prometheus/VM. This decision gates whether "drop-in VM replacement" is a
   claim or an approximation.
2. Unify grid alignment across engines to `start + n*step` (§2.7) — small,
   mechanical, high API-hygiene value.

**P1 — features real dashboards hit weekly:**
3. Vector matching `on()` / `ignoring()` / `group_left/right` — node_exporter
   and kube dashboards use these heavily (e.g. `metric * on(instance)
   group_left(nodename) node_uname_info`).
4. `histogram_quantile` over classic `_bucket`/`le` series — every latency
   panel. Implementable in the evaluator (group by `le`, interpolate).
5. `label_replace` / `label_join` — Grafana legend/relabel idioms; pure
   evaluator work.
6. Honest `increase` (multiply by window, not step) and `irate` (last two
   samples — needs raw tail access or a native `:irate` aggregate), rate
   reset-adjustment. Also unify the two engines' rate math (§2.3).
7. Respect range windows where window > step (bucket by window with stride
   step, or document permanently).

**P2 — breadth, mostly evaluator-local:**
8. Trivial transforms: `sgn`, trig family, `deg/rad/pi` (~30 lines total).
9. `scalar()`, `vector()`, `time()`, `timestamp()`, clock functions.
10. `delta/idelta/deriv/predict_linear/changes/resets` (gauge rollups; deriv &
    predict_linear are least-squares over the window — needs windowed points).
11. `quantile/stddev/stdvar_over_time` (need per-bucket raw values — candidate
    native aggregates).
12. `absent()`/`absent_over_time` (synthesize a 1-value series when empty).
13. `sort`/`sort_desc`/`sort_by_label*` (instant-query ordering), `count_values`,
    `limitk`/`limit_ratio`.
14. Duplicate-matcher AND semantics (§2.6) — store matchers as a list.

**P3 — large or niche:**
15. Subqueries `[5m:1m]`, `@` modifier.
16. Native histograms.
17. MetricsQL surface (`default`, `WITH`, `keep_metric_names`, rollup
    extensions) — only if VM drop-in is meant literally rather than
    "Prometheus-compatible on VM ports".
18. UTF-8 quoted metric names (Prometheus 3.x syntax).

## 4. Current totals (battery of 148 constructs)

72 supported · 76 rejected — all 76 now return a *named* error
("not supported yet" / "MetricsQL extension" / "unknown function"), none
return empty success. The 7 divergences in §2 are the remaining silent risk
surface; every one is either fixed by P0/P1 items or should be promoted into
module/API docs as documented behavior.
