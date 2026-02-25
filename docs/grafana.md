# Grafana Integration

TimelessMetrics exposes a Prometheus-compatible query endpoint that works with Grafana's built-in Prometheus data source.

## Adding as a data source

1. In Grafana, go to **Configuration > Data Sources > Add data source**
2. Select **Prometheus**
3. Set the URL to your TimelessMetrics instance with the `/prometheus` prefix:

   ```
   http://your-host:8428/prometheus
   ```

4. If authentication is enabled, add a custom header:
   - Header: `Authorization`
   - Value: `Bearer your-token`

5. Click **Save & Test**

## Supported PromQL patterns

The `/prometheus/api/v1/query_range` endpoint supports a subset of PromQL that covers the most common dashboard queries:

### Simple selectors

```promql
cpu_usage{hostname="host_0"}
```

### Regex label matching

```promql
cpu_usage{hostname=~"host_0|host_1"}
```

### Range vector functions

```promql
max_over_time(cpu_usage{hostname="host_0"}[1h])
avg_over_time(mem_usage{hostname="host_0"}[5m])
rate(http_requests_total{job="web"}[5m])
```

Supported functions:
- `avg_over_time`
- `min_over_time`
- `max_over_time`
- `sum_over_time`
- `count_over_time`
- `rate`
- `irate`

### Outer aggregation with group-by

```promql
max(max_over_time(cpu_usage{hostname=~"host_.*"}[1h])) by (hostname)
```

Supported outer aggregations: `avg`, `min`, `max`, `sum`, `count`

### Threshold filtering

```promql
max(max_over_time(cpu_usage{hostname="host_0"}[1h])) by (hostname) > 90
```

### Multi-metric with regex `__name__`

```promql
max(max_over_time({__name__=~"cpu_.*",hostname=~"host_0"}[1h])) by (hostname)
```

### Negative matching

```promql
cpu_usage{hostname!="host_0"}
cpu_usage{hostname!~"host_0|host_1"}
```

## Dashboard examples

### Time series panel

Create a panel with a Prometheus data source query:

```promql
avg_over_time(cpu_usage{hostname=~"$hostname"}[5m])
```

This returns a time series for each matching host, plotted as separate lines.

### Grouped aggregation

```promql
max(max_over_time(cpu_usage{region=~"$region"}[1h])) by (hostname)
```

### Rate queries

```promql
rate(http_requests_total{job="web"}[5m])
```

## Variable queries

Use Grafana's variable feature to create dynamic dropdowns:

1. Add a variable of type **Query**
2. Data source: your TimelessMetrics Prometheus source
3. Query type: **Label values**
4. Label: the label key (e.g., `hostname`)
5. Metric: the metric name (e.g., `cpu_usage`)

The label values endpoint is:
```
/api/v1/label/{label_name}/values?metric={metric_name}
```

### List all metric names

For a metric name variable, use label `__name__`:
```
/api/v1/label/__name__/values
```

## Annotation queries

Grafana can display TimelessMetrics annotations on panels:

1. Go to **Dashboard Settings > Annotations**
2. Add a new annotation source
3. Use a **Generic HTTP** data source or query the annotations API directly:

   ```
   http://your-host:8428/api/v1/annotations?from=${__from:date:seconds}&to=${__to:date:seconds}
   ```

4. Or filter by tags:

   ```
   http://your-host:8428/api/v1/annotations?from=${__from:date:seconds}&to=${__to:date:seconds}&tags=deploy
   ```

## Authentication setup

### Bearer token

If `bearer_token` is configured on the HTTP server:

1. In the Grafana data source configuration, under **Custom HTTP Headers**:
   - Header: `Authorization`
   - Value: `Bearer your-token`

### No authentication

If no bearer token is set, Grafana connects without authentication. This is suitable for trusted networks where the TimelessMetrics instance is not publicly accessible.

## Native query endpoint

In addition to the Prometheus-compatible endpoint, you can use TimelessMetrics' native `query_range` endpoint with Grafana's **JSON API** data source plugin:

```
http://your-host:8428/api/v1/query_range?metric=cpu_usage&from=${__from:date:seconds}&to=${__to:date:seconds}&step=${__interval_ms:raw}
```

This supports additional features like:
- `group_by` for cross-series aggregation
- `cross_aggregate` for specifying the cross-series function
- `metrics` for multi-metric queries
- `threshold_gt` / `threshold_lt` for server-side filtering
- `transform` for data transforms like `rate`
- `limit` for top-N queries

See [API Reference](API.md) for the full query_range documentation.
