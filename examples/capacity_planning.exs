# Capacity planning example: 1 year of daily data → 1 year forecast
# Run with: mix run examples/capacity_planning.exs

output_dir = "docs/examples"
File.mkdir_p!(output_dir)

:rand.seed(:exsss, {123, 456, 789})

now = System.os_time(:second)
now = div(now, 86_400) * 86_400
one_year = 365 * 86_400
start = now - one_year

IO.puts("=== Capacity Planning Demo ===\n")
IO.puts("Generating 1 year of daily bandwidth data...")

# Simulate ISP bandwidth growth: organic growth curve + weekly seasonality + noise
data =
  Stream.iterate(start, &(&1 + 86_400))
  |> Enum.take_while(&(&1 <= now))
  |> Enum.map(fn ts ->
    days_elapsed = (ts - start) / 86_400
    day_of_week = rem(div(ts, 86_400), 7)

    # Organic subscriber growth: 40 Gbps base, ~15% annual growth (quadratic)
    growth = 40.0 + 6.0 * (days_elapsed / 365)

    # Weekly pattern: weekends are ~8% higher (streaming)
    weekend_bump = if day_of_week in [5, 6], do: growth * 0.08, else: 0.0

    # Seasonal: summer is ~5% higher (more streaming, less school)
    month = rem(div(div(ts - start, 86_400), 30), 12) + 1
    seasonal = if month in [6, 7, 8], do: growth * 0.05, else: 0.0

    noise = :rand.normal() * 1.2

    {ts, Float.round(growth + weekend_bump + seasonal + noise, 2)}
  end)

IO.puts("  #{length(data)} daily data points")
IO.puts("  Range: #{elem(hd(data), 1)} Gbps → #{elem(List.last(data), 1)} Gbps")

# --- Forecast 1 year ahead ---
IO.puts("\nForecasting 1 year ahead...")

{us, {:ok, forecast}} =
  :timer.tc(fn ->
    Timeless.Forecast.predict(data, horizon: one_year, bucket: 86_400)
  end)

IO.puts("  Forecast: #{length(forecast)} points in #{div(us, 1000)}ms")
IO.puts("  Current: ~#{Float.round(elem(List.last(data), 1), 1)} Gbps")
IO.puts("  Predicted (6 months): ~#{Float.round(elem(Enum.at(forecast, 182), 1), 1)} Gbps")
IO.puts("  Predicted (12 months): ~#{Float.round(elem(List.last(forecast), 1), 1)} Gbps")

# Show auto-detected periods
periods = Timeless.Forecast.auto_periods(Enum.map(data, &elem(&1, 0)))
period_names = Enum.map(periods, fn
  604_800 -> "weekly (7d)"
  31_536_000 -> "yearly (365d)"
  p -> "#{div(p, 86_400)}d"
end)
IO.puts("  Auto-detected periods: #{Enum.join(period_names, ", ")}")

# --- Anomaly detection on historical data ---
IO.puts("\nRunning anomaly detection on historical data...")

{us2, {:ok, analysis}} = :timer.tc(fn -> Timeless.Anomaly.detect(data, sensitivity: :medium) end)
anomalies = Enum.filter(analysis, & &1.anomaly)
IO.puts("  Anomaly detection: #{div(us2, 1000)}ms")
IO.puts("  Flagged #{length(anomalies)} anomalous days out of #{length(data)}")

# --- Render capacity planning chart ---
IO.puts("\nRendering capacity planning chart...")

series = [%{labels: %{"region" => "east"}, data: data}]

# Connect forecast to last data point
last_point = List.last(data)
forecast_with_connection = [last_point | forecast]

anomaly_points =
  anomalies
  |> Enum.map(fn a -> {a.timestamp, a.value} end)

svg =
  Timeless.Chart.render("Peak Bandwidth (Gbps) — 1yr history + 1yr forecast", series,
    width: 1000,
    height: 400,
    theme: :light,
    forecast: forecast_with_connection,
    anomalies: anomaly_points
  )

File.write!(Path.join(output_dir, "chart_capacity_planning.svg"), svg)

# --- Also render dark theme version ---
svg_dark =
  Timeless.Chart.render("Peak Bandwidth (Gbps) — 1yr history + 1yr forecast", series,
    width: 1000,
    height: 400,
    theme: :dark,
    forecast: forecast_with_connection,
    anomalies: anomaly_points
  )

File.write!(Path.join(output_dir, "chart_capacity_dark.svg"), svg_dark)

# --- Capacity thresholds ---
IO.puts("\n=== Capacity Thresholds ===")
max_capacity = 60.0

for {months, label} <- [{3, "3 months"}, {6, "6 months"}, {9, "9 months"}, {12, "12 months"}] do
  idx = min(months * 30, length(forecast) - 1)
  predicted = elem(Enum.at(forecast, idx), 1)
  utilization = predicted / max_capacity * 100
  status = if utilization > 80, do: " ⚠ UPGRADE NEEDED", else: ""
  IO.puts("  #{label}: ~#{Float.round(predicted, 1)} Gbps (#{Float.round(utilization, 1)}% of #{max_capacity} Gbps#{status})")
end

IO.puts("\nDone! Charts written to #{output_dir}/")
