# Forecasting & Anomaly Detection

TimelessMetrics includes a built-in forecast engine and anomaly detector -- no external ML libraries needed. The implementation uses pure Elixir with a normal equation solver that runs in ~3ms for a year of daily data.

## How forecasting works

The forecast model fits a **polynomial trend** (degree 2) plus **Fourier seasonal terms** to historical data:

```
predicted(t) = a₀ + a₁·t + a₂·t² + Σ(bₖ·sin(2π·t/Pₖ) + cₖ·cos(2π·t/Pₖ))
```

Where `Pₖ` are the seasonal periods. Coefficients are solved via the normal equation `(X'X)⁻¹X'y` using Gaussian elimination with partial pivoting.

### Auto-detected seasonal periods

The model automatically selects seasonal periods based on the median sampling interval of the input data:

| Sampling interval | Periods | Use case |
|---|---|---|
| Sub-hourly (< 1h) | Daily (86400s) + half-daily (43200s) | Operational monitoring |
| Hourly (1h-23h) | Daily (86400s) + weekly (604800s) | Trend analysis |
| Daily (>= 1d) | Weekly (604800s) + yearly (31536000s) | Capacity planning |

### Custom seasonal periods

Override auto-detection with the `:periods` option (list of seconds):

```elixir
TimelessMetrics.Forecast.predict(data,
  horizon: 86_400,
  periods: [3600, 86_400])  # hourly + daily seasonality
```

### Minimum data requirements

The model needs at least `3 + 2 * number_of_periods` data points. With the default 2 periods, that's 7 points minimum. Returns `{:error, :insufficient_data}` if there aren't enough.

## Elixir API

### Forecast

```elixir
now = System.os_time(:second)

# Forecast 6 hours ahead from 24 hours of 5-minute data
{:ok, results} = TimelessMetrics.forecast(:metrics, "cpu_usage", %{"host" => "web-1"},
  from: now - 86_400,
  horizon: 21_600,
  bucket: {300, :seconds},
  aggregate: :avg)
```

Returns:

```elixir
{:ok, [
  %{
    labels: %{"host" => "web-1"},
    data: [{1700000000, 73.2}, {1700000300, 74.1}, ...],      # historical
    forecast: [{1700086400, 72.8}, {1700086700, 73.5}, ...]    # predicted
  }
]}
```

**Options:**

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `:from` | integer | yes | Start of historical data (unix seconds) |
| `:to` | integer | no | End of historical data (default: now) |
| `:horizon` | integer | yes | Seconds to forecast ahead |
| `:bucket` | tuple/atom | no | Bucket size (default: `{300, :seconds}`) |
| `:aggregate` | atom | no | Aggregate function (default: `:avg`) |

### Anomaly detection

```elixir
{:ok, results} = TimelessMetrics.detect_anomalies(:metrics, "cpu_usage", %{"host" => "web-1"},
  from: now - 86_400,
  bucket: {300, :seconds},
  sensitivity: :medium)
```

Returns:

```elixir
{:ok, [
  %{
    labels: %{"host" => "web-1"},
    analysis: [
      %{timestamp: 1700000000, value: 73.2, expected: 72.8, score: 0.45, anomaly: false},
      %{timestamp: 1700000300, value: 98.7, expected: 74.1, score: 3.12, anomaly: true},
      ...
    ]
  }
]}
```

**Options:**

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `:from` | integer | yes | Start timestamp (unix seconds) |
| `:to` | integer | no | End timestamp (default: now) |
| `:bucket` | tuple/atom | no | Bucket size (default: `{300, :seconds}`) |
| `:aggregate` | atom | no | Aggregate function (default: `:avg`) |
| `:sensitivity` | atom | no | `:low`, `:medium`, or `:high` (default: `:medium`) |

### Low-level API

For direct model access:

```elixir
# Predict from raw data tuples
data = [{1700000000, 73.2}, {1700000300, 74.1}, ...]
{:ok, predictions} = TimelessMetrics.Forecast.predict(data,
  horizon: 3600,
  bucket: 300)

# Fit and predict on training data (used by anomaly detection)
{:ok, fitted_values} = TimelessMetrics.Forecast.fit_predict(data)
```

## HTTP API

### Forecast

```bash
curl 'http://localhost:8428/api/v1/forecast?metric=cpu_usage&host=web-1&from=-24h&step=300&horizon=6h'
```

**Query parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `metric` | **(required)** | Metric name |
| `from` | `-1h` | Start time (unix seconds or relative like `-24h`) |
| `to` | `now` | End time |
| `step` | `300` | Bucket size in seconds |
| `horizon` | `3600` | Forecast duration (supports `6h`, `1d`, etc.) |
| `transform` | -- | Optional transform (e.g., `rate`) |

Additional parameters become label filters (e.g., `&host=web-1`).

Response:

```json
{
  "metric": "cpu_usage",
  "series": [
    {
      "labels": {"host": "web-1"},
      "data": [[1700000000, 73.2], [1700000300, 74.1]],
      "forecast": [[1700086400, 72.8], [1700086700, 73.5]]
    }
  ]
}
```

### Anomaly detection

```bash
curl 'http://localhost:8428/api/v1/anomalies?metric=cpu_usage&host=web-1&from=-24h&step=300&sensitivity=medium'
```

**Query parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `metric` | **(required)** | Metric name |
| `from` | `-1h` | Start time |
| `to` | `now` | End time |
| `step` | `300` | Bucket size in seconds |
| `sensitivity` | `medium` | Sensitivity level: `low`, `medium`, or `high` |
| `transform` | -- | Optional transform |

Response:

```json
{
  "metric": "cpu_usage",
  "series": [
    {
      "labels": {"host": "web-1"},
      "analysis": [
        {"timestamp": 1700000000, "value": 73.2, "expected": 72.8, "score": 0.45, "anomaly": false},
        {"timestamp": 1700000300, "value": 98.7, "expected": 74.1, "score": 3.12, "anomaly": true}
      ]
    }
  ]
}
```

## Sensitivity levels

Anomaly detection uses z-score analysis on residuals (actual - predicted). A point is flagged as anomalous when its absolute z-score exceeds the threshold:

| Level | Z-score threshold | Behavior |
|-------|-------------------|----------|
| `:low` | 3.0 | Only flags extreme outliers |
| `:medium` | 2.5 | Good balance (default) |
| `:high` | 2.0 | Flags more subtle anomalies |

## Chart overlays

Both forecasts and anomalies can be overlaid on SVG charts via the `/chart` endpoint:

```html
<!-- Purple dashed forecast line -->
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-24h&forecast=6h" />

<!-- Red anomaly dots -->
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-24h&anomalies=medium" />

<!-- Both together -->
<img src="http://localhost:8428/chart?metric=cpu_usage&from=-24h&forecast=6h&anomalies=medium" />
```

The forecast appears as a purple dashed line extending beyond the historical data. Anomalies appear as red dots on the data points that were flagged.

## Interpreting results

### Forecast output

- **`data`**: the historical time series used to build the model
- **`forecast`**: predicted future values at the same bucket interval
- Forecasts work best with regular, periodic data (CPU usage, network traffic, request rates)
- The model captures trend (linear + quadratic) and seasonality but not sudden regime changes

### Anomaly output

- **`expected`**: what the model predicted for that timestamp
- **`score`**: absolute z-score (how many standard deviations from normal)
- **`anomaly`**: `true` if the score exceeds the sensitivity threshold
- High scores indicate points that deviate significantly from the seasonal pattern
- Consider using `:low` sensitivity for noisy metrics and `:high` for stable metrics

### Capacity planning

For long-range forecasting (months to years), use daily-bucketed data:

```elixir
{:ok, results} = TimelessMetrics.forecast(:metrics, "bandwidth_peak_mbps", %{},
  from: now - 365 * 86_400,
  horizon: 365 * 86_400,
  bucket: :day,
  aggregate: :max)
```

See [Capacity Planning](capacity_planning.md) for detailed ISP-focused examples.
