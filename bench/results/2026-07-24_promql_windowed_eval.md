# PromQL windowed evaluator — baseline (6.2.1)

Host: i7-14700HX (linux), Rust engine, 200 series x 5760 pts (1.15M points,
24h @ 15s), warm runs. `bench/promql_windowed_bench.exs`.

| Query | Range/step | Time |
|---|---|---|
| `cpu` (200 series) | 1h / 60s | 12ms |
| `cpu` | 24h / 300s | 251ms |
| `rate(cpu[5m])` | 24h / 300s | 371ms |
| `sum by (host) (rate(cpu[5m]))` | 24h / 300s | 423ms |
| `avg(cpu)` | 24h / 300s | 219ms |
| `cpu / 10` | 24h / 300s | 275ms |
| `max_over_time(cpu[1h])` | 6h / 60s | 472ms |
| `max_over_time(cpu[6h])` | 6h / 60s | **2717ms** |
| `avg_over_time(cpu[1h])` | 6h / 60s | 719ms |
| `max_over_time(rate(cpu[5m])[1h:1m])` | 6h / 300s | 335ms |

Reading:

- Dashboard-shaped load (window ≈ step) is O(points): the classic panel is
  12ms; full-day/200-series heavyweights are 250-420ms. No optimization
  needed.
- The known hot spot is window >> step: slice materialization is
  O(points x window/step) — the 6h-window/60s-step case (360x overlap) costs
  2.7s. Rare in real dashboards (Grafana scales windows with step).
- If the gap radar or a real workload ever hits it: incremental sliding
  aggregates (sum/avg/count are O(1)/step; min/max need a monotonic deque)
  would flatten that row to roughly the 1h-window cost. Alternatively it is
  exactly the Q2(b) pushdown kernel from the libsql plan's query-tier table.
- Fetch volume did NOT regress vs the pre-6.2 bucketed path: that path also
  fetched raw points and bucketed in Elixir. The windowed evaluator adds only
  the window head-room fetch and the overlap slicing above.

## Update (same day): parallel per-series evaluation

Changes: per-series grid evaluation fans out via Task.async_stream
(schedulers-wide); the redundant re-sort of engine-sorted points became an
allocation-free sorted check; multi-metric pattern fetches and subquery
inner evaluation parallelize the same way. 182/182 VM parity retained.

| Query | Before | After |
|---|---|---|
| `cpu` 1h/60s | 12ms | 10ms |
| `cpu` 24h/300s | 251ms | 236ms |
| `rate(cpu[5m])` 24h/300s | 371ms | 244ms |
| `sum by (host) (rate(cpu[5m]))` 24h/300s | 423ms | **188ms** |
| `avg(cpu)` 24h/300s | 219ms | 164ms |
| `cpu / 10` 24h/300s | 275ms | 187ms |
| `max_over_time(cpu[1h])` 6h/60s | 472ms | **52ms** |
| `max_over_time(cpu[6h])` 6h/60s | 2717ms | **254ms** |
| `avg_over_time(cpu[1h])` 6h/60s | 719ms | 63ms |
| subquery 6h/300s | 335ms | 73ms |

Remaining floor for the 24h/200-series class (~190-240ms): the single
serial NIF fetch (~71ms for 1.15M points), the inter-process copy of raw
point lists into eval tasks, and response string formatting. Compute is no
longer the bottleneck — further improvement belongs to the Q2 pushdown
kernels (grid-last, windowed aggregates) planned for the storage engine
swap, which eliminate both the fetch volume and the copies.
