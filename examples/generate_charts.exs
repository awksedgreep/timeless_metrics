# Generate example SVG charts demonstrating forecast and anomaly detection.
# Run with: mix run examples/generate_charts.exs

output_dir = "docs/examples"
File.mkdir_p!(output_dir)

# Seed for reproducible output
:rand.seed(:exsss, {42, 42, 42})

now = System.os_time(:second)
now = div(now, 300) * 300
start = now - 48 * 3600

IO.puts("Generating 48h of realistic ISP metric data...")

# Generate realistic CPU usage data: daily pattern + trend + noise
raw_data =
  Stream.iterate(start, &(&1 + 300))
  |> Enum.take_while(&(&1 <= now))
  |> Enum.map(fn ts ->
    hour = rem(div(ts, 3600), 24)

    # Daily pattern: business hours peak (8am-6pm)
    daily =
      if hour >= 8 and hour <= 18 do
        15.0 * :math.sin((hour - 8) * :math.pi() / 10)
      else
        0.0
      end

    # Slight upward trend over 48h
    trend = (ts - start) / (48 * 3600) * 5.0
    base = 38.0 + daily + trend
    noise = :rand.normal() * 2.5

    {ts, Float.round(base + noise, 2)}
  end)

# Inject anomaly spikes at known offsets
anomaly_offsets = [8 * 3600, 24 * 3600, 38 * 3600]
anomaly_times = Enum.map(anomaly_offsets, &(start + &1))

data =
  Enum.map(raw_data, fn {ts, val} ->
    if Enum.any?(anomaly_times, &(abs(ts - &1) < 300)) do
      {ts, Float.round(val + 25 + :rand.normal() * 3, 2)}
    else
      {ts, val}
    end
  end)

IO.puts("  #{length(data)} data points generated")

series = [%{labels: %{"host" => "web-1"}, data: data}]

# --- Chart 1: Basic ---
IO.puts("Rendering basic chart...")

svg_basic =
  Timeless.Chart.render("cpu_usage", series,
    width: 900,
    height: 350,
    theme: :light,
    forecast: [],
    anomalies: []
  )

File.write!(Path.join(output_dir, "chart_basic.svg"), svg_basic)

# --- Chart 2: With Forecast ---
IO.puts("Rendering forecast chart...")

{:ok, forecast_points} = Timeless.Forecast.predict(data, horizon: 6 * 3600, bucket: 300)

# Connect forecast to last data point
last_point = List.last(data)
forecast_with_connection = [last_point | forecast_points]

svg_forecast =
  Timeless.Chart.render("cpu_usage — 6h forecast", series,
    width: 900,
    height: 350,
    theme: :light,
    forecast: forecast_with_connection,
    anomalies: []
  )

File.write!(Path.join(output_dir, "chart_forecast.svg"), svg_forecast)

IO.puts("  Forecast: #{length(forecast_points)} predicted points")

# --- Chart 3: With Anomaly Detection ---
IO.puts("Rendering anomaly detection chart...")

{:ok, analysis} = Timeless.Anomaly.detect(data, sensitivity: :medium)

anomalies =
  analysis
  |> Enum.filter(& &1.anomaly)
  |> Enum.map(fn a -> {a.timestamp, a.value} end)

svg_anomalies =
  Timeless.Chart.render("cpu_usage — anomaly detection", series,
    width: 900,
    height: 350,
    theme: :light,
    forecast: [],
    anomalies: anomalies
  )

File.write!(Path.join(output_dir, "chart_anomalies.svg"), svg_anomalies)

IO.puts("  Detected #{length(anomalies)} anomalies out of #{length(data)} points")

# --- Chart 4: Full Analysis (forecast + anomalies + annotation) ---
IO.puts("Rendering full analysis chart...")

deploy_annotation = %{
  timestamp: start + 30 * 3600,
  title: "Deploy v2.1"
}

svg_full =
  Timeless.Chart.render("cpu_usage — full analysis", series,
    width: 900,
    height: 350,
    theme: :light,
    forecast: forecast_with_connection,
    anomalies: anomalies,
    annotations: [deploy_annotation]
  )

File.write!(Path.join(output_dir, "chart_full_analysis.svg"), svg_full)

# --- Chart 5: Dark theme ---
IO.puts("Rendering dark theme chart...")

svg_dark =
  Timeless.Chart.render("cpu_usage — dark theme", series,
    width: 900,
    height: 350,
    theme: :dark,
    forecast: forecast_with_connection,
    anomalies: anomalies,
    annotations: [deploy_annotation]
  )

File.write!(Path.join(output_dir, "chart_dark.svg"), svg_dark)

IO.puts("")
IO.puts("Generated #{length(Path.wildcard(Path.join(output_dir, "*.svg")))} SVG files in #{output_dir}/")

for f <- Path.wildcard(Path.join(output_dir, "*.svg")) |> Enum.sort() do
  size = File.stat!(f).size
  IO.puts("  #{Path.basename(f)} (#{Float.round(size / 1024, 1)} KB)")
end
