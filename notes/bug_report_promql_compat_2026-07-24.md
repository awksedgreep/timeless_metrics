# Bug report: PromQL compatibility gaps break VictoriaMetrics API consumers

**Date:** 2026-07-24
**Found by:** ddnet poller dashboards (Chart.js panels querying `/api/v1/query_range`)
**Deployment:** `timeless-stack` container, release 0.6.11, VM-compatible API on :8428
**Severity:** High for drop-in VM replacement — queries that work on VictoriaMetrics silently return empty results, so every dashboard panel rendered a blank chart with no error.

## Summary

ddnet's dashboards were previously backed by VictoriaMetrics and build PromQL like
`avg by (host) (cpu_usage)` and `(avg(cpu_usage)) / 10`. After timeless-stack
replaced VM on :8428, all of these return `{"status":"success","data":{"result":[]}}`
— success with empty results, not a parse error — which made the breakage
invisible to the client. Plain selectors, label filters, `rate()`, and
`avg_over_time()` work fine.

## Repro setup

Seed sample data (import endpoint works correctly):

```sh
# ddnet repo: writes cpu_usage etc. for 5 hosts via /api/v1/import/prometheus
VICTORIA_URL=http://localhost:8428 elixir scripts/populate_victoria_metrics.exs
```

All queries below use GET with `start=$(date +%s -d '-2 hours')`, `end=$(date +%s)`, `step=60s`:

```sh
q() { curl -s -G "http://localhost:8428/api/v1/query_range" \
  --data-urlencode "query=$1" \
  -d "start=$(date +%s -d '-2 hours')" -d "end=$(date +%s)" -d "step=60s"; }
```

## Findings

| Query | VictoriaMetrics | timeless-stack 0.6.11 |
|---|---|---|
| `cpu_usage` | series per host | ✅ same |
| `cpu_usage{host="router1"}` | 1 series | ✅ same |
| `rate(ifHCInOctets[5m])` | rate series | ✅ works |
| `avg_over_time(cpu_usage[5m])` | smoothed series | ✅ works |
| `avg by (host) (cpu_usage)` | 1 series per host | ❌ **empty result** |
| `avg(cpu_usage)` | **1** aggregated series | ⚠️ returns **5** per-host series (aggregation is a per-series no-op, labels kept incl. `__name__`) |
| `last_over_time(cpu_usage[5m])` | series | ❌ **empty result** |
| `cpu_usage / 10` (any binary arithmetic: `* 100`, `(expr) / 10`, …) | scaled series | ❌ **empty result** |
| POST to `/api/v1/query_range` (form body) | supported | ❌ **404 "not found"** (GET-only routes) |

Also affected: `sum/min/max/count by (...)` — any `by`/`without` grouping returns empty.

## Why the empty-success responses hurt

Prometheus/VM return HTTP 400 with an error body for unsupported/invalid queries.
Returning `status: success` + `result: []` is indistinguishable from "metric has
no data", so clients (Grafana included) render empty charts instead of surfacing
an error. If a construct isn't supported, an `{"status":"error","error":"..."}`
response would let clients degrade gracefully.

## Suggested priorities

1. **Return a parse/unsupported error instead of empty success** — cheapest fix,
   makes every other gap debuggable.
2. **`by (...)` grouping for aggregation operators** — this is the single most
   common shape Grafana and hand-rolled dashboards emit (`avg by (host) (m)`).
3. **Cross-series aggregation semantics** — `avg(m)` should collapse to one
   series (and drop `__name__`), not pass through per-series.
4. **Scalar binary arithmetic** (`expr / N`, `expr * N`) — used for unit
   conversions everywhere.
5. **`last_over_time`** — used for stat/gauge panels.
6. **POST support on `/api/v1/query`, `/api/v1/query_range`** — Prometheus API
   spec allows both; Grafana uses POST for long queries.

## Workaround applied in ddnet

`Poller.Metrics.VictoriaReader` (ddnet repo) now retries with a plain selector
when an aggregated query returns an empty result, and applies simple `OP NUMBER`
transforms client-side. That unblocks the dashboards but loses true cross-series
aggregation until items 2–3 land.
