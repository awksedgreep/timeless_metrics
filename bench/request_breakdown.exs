# Benchmarks individual components of the Prometheus import request path.
# Run: mix run bench/request_breakdown.exs

data_dir = "/tmp/timeless_breakdown_#{System.os_time(:millisecond)}"
File.mkdir_p!(data_dir)
{:ok, _} = TimelessMetrics.Supervisor.start_link(name: :breakdown, data_dir: data_dir, self_monitor: false, scraping: false)

IO.puts("=== Request Path Breakdown ===\n")

# Build a realistic 50-line Prometheus text body
body =
  Enum.map_join(1..50, "\n", fn i ->
    "bench_metric_#{rem(i, 20)}{host=\"device_#{String.pad_leading(Integer.to_string(i), 6, "0")}\",env=\"prod\"} #{:rand.uniform() * 100} #{System.os_time(:millisecond)}"
  end)

iterations = 5000
IO.puts("Body: #{byte_size(body)} bytes, 50 lines, #{iterations} iterations\n")

# 1. Prometheus NIF parse
nif_available = TimelessMetrics.PrometheusNif.available?()
IO.puts("PrometheusNif available: #{nif_available}")

if nif_available do
  {us, _} = :timer.tc(fn ->
    for _ <- 1..iterations do
      TimelessMetrics.PrometheusNif.parse(body)
    end
  end)
  IO.puts("  NIF parse:          #{Float.round(us / iterations, 1)}us/call")
end

# 2. Elixir parser (sequential)
lines = :binary.split(body, <<"\n">>, [:global, :trim_all])
{us, _} = :timer.tc(fn ->
  for _ <- 1..iterations do
    Enum.reduce(lines, {%{}, 0, 0, []}, fn line, {groups, count, errors, samples} ->
      # Minimal parse — just split on space
      case String.split(line, " ") do
        [_metric_labels, _value, _ts] -> {groups, count + 1, errors, samples}
        _ -> {groups, count, errors + 1, samples}
      end
    end)
  end
end)
IO.puts("  Elixir split parse: #{Float.round(us / iterations, 1)}us/call")

# 3. Series resolution (50 lookups)
registry = :breakdown_registry
# Warm up — create all series first
for i <- 1..50 do
  TimelessMetrics.SeriesRegistry.get_or_create(registry, "bench_metric_#{rem(i, 20)}", %{"host" => "device_#{String.pad_leading(Integer.to_string(i), 6, "0")}", "env" => "prod"})
end
# Force publish so persistent_term is populated
TimelessMetrics.SeriesRegistry.flush_pending(registry)
Process.sleep(6000)  # Wait for publish timer

series_keys = for i <- 1..50 do
  {"bench_metric_#{rem(i, 20)}", %{"host" => "device_#{String.pad_leading(Integer.to_string(i), 6, "0")}", "env" => "prod"}}
end

{us, _} = :timer.tc(fn ->
  for _ <- 1..iterations do
    Enum.each(series_keys, fn {metric, labels} ->
      TimelessMetrics.SeriesRegistry.get_or_create(registry, metric, labels)
    end)
  end
end)
IO.puts("  Series resolve (50): #{Float.round(us / iterations, 1)}us/call")

# 4. Buffer write (50 points to shards)
shard_count = :persistent_term.get({TimelessMetrics, :breakdown, :shard_count})
points_by_shard = series_keys
  |> Enum.map(fn {metric, labels} ->
    sid = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric, labels)
    {sid, System.os_time(:second), :rand.uniform() * 100}
  end)
  |> Enum.group_by(fn {sid, _, _} -> rem(abs(sid), shard_count) end)

{us, _} = :timer.tc(fn ->
  for _ <- 1..iterations do
    Enum.each(points_by_shard, fn {shard_idx, points} ->
      TimelessMetrics.Buffer.write_bulk(:"breakdown_shard_#{shard_idx}", points)
    end)
  end
end)
IO.puts("  Buffer write (50):  #{Float.round(us / iterations, 1)}us/call")

# 5. Full end-to-end via HTTP
{:ok, _} = Rocket.start_link(port: 19428, handler: TimelessMetrics.HTTP, max_body: 10 * 1024 * 1024)
:persistent_term.put({TimelessMetrics.HTTP, :config}, {:breakdown, nil})
Application.ensure_all_started(:inets)
Process.sleep(500)

charlist_body = String.to_charlist(body)
{us, _} = :timer.tc(fn ->
  for _ <- 1..iterations do
    {:ok, {{_, 204, _}, _, _}} =
      :httpc.request(:post, {~c"http://127.0.0.1:19428/api/v1/import/prometheus", [], ~c"text/plain", charlist_body}, [{:timeout, 5000}], [])
  end
end)
IO.puts("  HTTP round-trip:    #{Float.round(us / iterations, 1)}us/call")

IO.puts("\n  HTTP overhead:      #{Float.round(us / iterations - (if nif_available, do: 0, else: 0), 1)}us (total - parse - resolve - write)")

# Cleanup
File.rm_rf!(data_dir)
