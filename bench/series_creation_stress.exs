# Benchmarks series creation speed with a fresh store + HTTP writes.
# Measures how fast 200K new series can be created via Prometheus text import.

data_dir = "/tmp/timeless_series_bench_#{System.os_time(:millisecond)}"
File.mkdir_p!(data_dir)

{:ok, _} = TimelessMetrics.Supervisor.start_link(name: :bench_store, data_dir: data_dir, engine: :actor)
:persistent_term.put({TimelessMetrics.HTTP, :config}, {:bench_store, nil})
{:ok, _} = Rocket.start_link(port: 9428, handler: TimelessMetrics.HTTP, max_body: 10 * 1024 * 1024)

IO.puts("Store ready on port 9428 — data_dir: #{data_dir}")
IO.puts("Sending 200K unique series via Prometheus text format...\n")

# Build 200K unique series in batches of 50
devices = 10_000
metrics = 20
batch_size = 50
total = devices * metrics
ts_ms = System.os_time(:millisecond)

series =
  for d <- 1..devices, m <- 0..(metrics - 1) do
    "bench.metric_#{m}{host=\"device_#{String.pad_leading(Integer.to_string(d), 6, "0")}\",env=\"prod\"}"
  end

batches = Enum.chunk_every(series, batch_size)

IO.puts("#{total} series in #{length(batches)} batches of #{batch_size}")

# Send all batches sequentially, timing the creation
{creation_us, _} =
  :timer.tc(fn ->
    Enum.each(batches, fn batch ->
      body =
        Enum.map_join(batch, "\n", fn line ->
          "#{line} #{:rand.uniform() * 100} #{ts_ms}"
        end)

      req = :httpc.request(:post, {~c"http://127.0.0.1:9428/api/v1/import/prometheus", [], ~c"text/plain", body}, [], [])

      case req do
        {:ok, {{_, status, _}, _, _}} when status in [200, 204] -> :ok
        other -> IO.puts("ERROR: #{inspect(other)}")
      end
    end)
  end)

creation_ms = creation_us / 1000
creation_s = creation_ms / 1000
series_per_sec = total / creation_s

IO.puts("\n=== Series Creation Results ===")
IO.puts("Total series:    #{total}")
IO.puts("Creation time:   #{Float.round(creation_s, 2)}s")
IO.puts("Series/sec:      #{Float.round(series_per_sec, 0)}")

# Check health
health = :httpc.request(~c"http://127.0.0.1:9428/health")
IO.puts("\nHealth: #{inspect(health)}")

# Cleanup
File.rm_rf!(data_dir)
