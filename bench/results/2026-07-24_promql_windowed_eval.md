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
