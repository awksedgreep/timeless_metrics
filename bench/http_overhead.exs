# Benchmark: HTTP layer overhead breakdown
#
# Measures each layer independently to find where time goes:
# 1. Raw HTTP round-trip (health endpoint — minimal work)
# 2. Prometheus text parsing
# 3. PromQL parsing
# 4. JSON encoding
# 5. Full write via HTTP
# 6. Full query via HTTP
# 7. Engine query directly (no HTTP)
#
# Usage:
#   mix run bench/http_overhead.exs

alias TimelessMetrics.{PromQL, Stats}

data_dir = Path.join(System.tmp_dir!(), "tm_http_overhead_#{System.unique_integer([:positive])}")
File.mkdir_p!(data_dir)

{:ok, _pid} =
  TimelessMetrics.Supervisor.start_link(
    name: :bench_store,
    data_dir: data_dir,
    self_monitor: false,
    scraping: false
  )

# Start HTTP server on a separate port pointed at bench_store
:persistent_term.put({TimelessMetrics.HTTP, :config}, {:bench_store, nil})
{:ok, _} = Rocket.start_link(port: 9428, handler: TimelessMetrics.HTTP, max_body: 10 * 1024 * 1024)

Process.sleep(500)

IO.puts("=== HTTP Overhead Breakdown ===\n")

# Pre-seed 1000 series with data
IO.puts("Seeding 1000 series with data...")
manager = :bench_store_actor_manager

for i <- 1..1000 do
  metric = "node_cpu_seconds_total"
  host = "device_#{String.pad_leading(Integer.to_string(i), 6, "0")}"
  labels = %{"host" => host, "region" => "us-east", "env" => "prod"}
  {_id, pid} = TimelessMetrics.Actor.SeriesManager.get_or_start(manager, metric, labels)

  now = System.os_time(:second)
  batch = for t <- (now - 3600)..now//15, do: {t, :rand.uniform() * 100.0}
  send(pid, {:write_batch, batch})
end

Process.sleep(2000)
IO.puts("Seeded.\n")

# Finch pool for HTTP calls
Finch.start_link(name: :BenchFinch, pools: %{default: [size: 32, count: 1]})

defmodule Measure do
  def run(label, iterations, fun) do
    # Warmup
    for _ <- 1..min(iterations, 100), do: fun.()

    times =
      for _ <- 1..iterations do
        {us, _} = :timer.tc(fun)
        us
      end

    sorted = Enum.sort(times)
    p50 = Enum.at(sorted, div(length(sorted), 2))
    p99 = Enum.at(sorted, trunc(length(sorted) * 0.99))
    avg = div(Enum.sum(times), length(times))
    min_v = hd(sorted)

    IO.puts(
      "  #{String.pad_trailing(label, 40)} min=#{pad(min_v)}  p50=#{pad(p50)}  p99=#{pad(p99)}  avg=#{pad(avg)}"
    )
  end

  defp pad(us) do
    str =
      cond do
        us >= 1_000_000 -> "#{Float.round(us / 1_000_000, 2)}s"
        us >= 1_000 -> "#{Float.round(us / 1_000, 2)}ms"
        true -> "#{us}us"
      end

    String.pad_leading(str, 10)
  end
end

n = 200

IO.puts("--- HTTP Round-trip (#{n} iterations) ---")

Measure.run("GET /health (baseline RT)", n, fn ->
  Finch.build(:get, "http://127.0.0.1:9428/health")
  |> Finch.request(:BenchFinch)
end)

# Write payloads
prom_1line = ~s(node_cpu_seconds_total{host="device_000001",region="us-east",env="prod"} 42.5 #{System.os_time(:millisecond)}\n)

prom_20lines =
  for i <- 1..20, into: "" do
    ~s(node_cpu_seconds_total{host="device_#{String.pad_leading(Integer.to_string(i), 6, "0")}",region="us-east",env="prod"} #{:rand.uniform() * 100} #{System.os_time(:millisecond)}\n)
  end

prom_1000lines =
  for i <- 1..1000, into: "" do
    ~s(node_cpu_seconds_total{host="device_#{String.pad_leading(Integer.to_string(i), 6, "0")}",region="us-east",env="prod"} #{:rand.uniform() * 100} #{System.os_time(:millisecond)}\n)
  end

IO.puts("\n--- Write via HTTP ---")

Measure.run("POST /import/prometheus (1 line)", n, fn ->
  Finch.build(:post, "http://127.0.0.1:9428/api/v1/import/prometheus", [{"content-type", "text/plain"}], prom_1line)
  |> Finch.request(:BenchFinch)
end)

Measure.run("POST /import/prometheus (20 lines)", n, fn ->
  Finch.build(:post, "http://127.0.0.1:9428/api/v1/import/prometheus", [{"content-type", "text/plain"}], prom_20lines)
  |> Finch.request(:BenchFinch)
end)

Measure.run("POST /import/prometheus (1000 lines)", n, fn ->
  Finch.build(:post, "http://127.0.0.1:9428/api/v1/import/prometheus", [{"content-type", "text/plain"}], prom_1000lines)
  |> Finch.request(:BenchFinch)
end)

IO.puts("\n--- Query via HTTP ---")

query = ~s(node_cpu_seconds_total{host="device_000001"})
now = System.os_time(:second)

Measure.run("GET /api/v1/query_range (PromQL, 1h)", n, fn ->
  Finch.build(:get, "http://127.0.0.1:9428/api/v1/query_range?query=#{URI.encode_www_form(query)}&start=#{now - 3600}&end=#{now}&step=60")
  |> Finch.request(:BenchFinch)
end)

Measure.run("GET /api/v1/query_range (native, 1h)", n, fn ->
  Finch.build(:get, "http://127.0.0.1:9428/api/v1/query_range?metric=node_cpu_seconds_total&host=device_000001&start=#{now - 3600}&end=#{now}&step=60")
  |> Finch.request(:BenchFinch)
end)

IO.puts("\n--- PromQL parse only (no HTTP) ---")

Measure.run("PromQL.parse simple selector", n, fn ->
  PromQL.parse(query)
end)

IO.puts("\n--- Engine query directly (no HTTP) ---")

Measure.run("Engine.query_aggregate_multi", n, fn ->
  TimelessMetrics.Actor.Engine.query_aggregate_multi(
    :bench_store,
    "node_cpu_seconds_total",
    %{"host" => "device_000001"},
    from: now - 3600,
    to: now,
    bucket: {60, :seconds},
    aggregate: :avg
  )
end)

IO.puts("\n--- JSON encoding ---")

{:ok, sample_result} =
  TimelessMetrics.Actor.Engine.query_aggregate_multi(
    :bench_store,
    "node_cpu_seconds_total",
    %{"host" => "device_000001"},
    from: now - 3600,
    to: now,
    bucket: {60, :seconds},
    aggregate: :avg
  )

prom_response = %{
  "status" => "success",
  "data" => %{
    "resultType" => "matrix",
    "result" =>
      Enum.map(sample_result, fn %{labels: labels, data: data} ->
        %{
          "metric" => labels,
          "values" => Enum.map(data, fn {ts, val} -> [ts, Float.to_string(val)] end)
        }
      end)
  }
}

Measure.run("JSON encode query response", n, fn ->
  :json.encode(prom_response)
end)

# Fast path stats
stats = Stats.snapshot(:bench_store)
fast = stats.query_fast_path
slow = stats.query_slow_path
total_lookups = fast + slow
hit_rate = if total_lookups > 0, do: Float.round(fast / total_lookups * 100, 1), else: 0.0

IO.puts("\n--- Read Path Stats ---")
IO.puts("  Fast path (ETS):        #{fast}")
IO.puts("  Slow path (GenServer):  #{slow}")
IO.puts("  Hit rate:               #{hit_rate}%")

File.rm_rf!(data_dir)
