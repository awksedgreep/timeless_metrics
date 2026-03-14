# Measures Rocket overhead: no-op handler vs actual handler vs raw socket
# Run: mix run bench/rocket_overhead.exs

Application.ensure_all_started(:inets)

# Minimal handler that returns 204 immediately
defmodule NoopHandler do
  def handle(req) do
    Rocket.Response.send_resp(req, 204, "")
  end
end

{:ok, _} = Rocket.start_link(port: 19001, handler: NoopHandler, max_body: 10 * 1024 * 1024)

body = String.duplicate("x", 4000)
charlist_body = String.to_charlist(body)
iterations = 10_000

IO.puts("=== Rocket Overhead ===\n")
IO.puts("Body size: #{byte_size(body)} bytes, #{iterations} iterations\n")

# Warmup
for _ <- 1..100 do
  {:ok, _} = :httpc.request(:post, {~c"http://127.0.0.1:19001/test", [], ~c"text/plain", charlist_body}, [], [])
end

# Noop handler — measures pure Rocket + HTTP overhead
{us, _} = :timer.tc(fn ->
  for _ <- 1..iterations do
    {:ok, {{_, 204, _}, _, _}} =
      :httpc.request(:post, {~c"http://127.0.0.1:19001/test", [], ~c"text/plain", charlist_body}, [], [])
  end
end)
IO.puts("  Noop handler (Rocket + HTTP):  #{Float.round(us / iterations, 1)}us/req")

# Now test with actual Timeless handler
data_dir = "/tmp/timeless_rocket_bench_#{System.os_time(:millisecond)}"
File.mkdir_p!(data_dir)
{:ok, _} = TimelessMetrics.Supervisor.start_link(name: :rocket_bench, data_dir: data_dir, self_monitor: false, scraping: false)
:persistent_term.put({TimelessMetrics.HTTP, :config}, {:rocket_bench, nil})
{:ok, _} = Rocket.start_link(port: 19002, handler: TimelessMetrics.HTTP, max_body: 10 * 1024 * 1024)

prom_body = Enum.map_join(1..50, "\n", fn i ->
  "bench_m#{rem(i,20)}{h=\"d#{i}\",e=\"p\"} #{:rand.uniform()*100} #{System.os_time(:millisecond)}"
end)
prom_charlist = String.to_charlist(prom_body)

# Warmup
for _ <- 1..100 do
  {:ok, _} = :httpc.request(:post, {~c"http://127.0.0.1:19002/api/v1/import/prometheus", [], ~c"text/plain", prom_charlist}, [], [])
end

{us, _} = :timer.tc(fn ->
  for _ <- 1..iterations do
    {:ok, {{_, 204, _}, _, _}} =
      :httpc.request(:post, {~c"http://127.0.0.1:19002/api/v1/import/prometheus", [], ~c"text/plain", prom_charlist}, [], [])
  end
end)
IO.puts("  Timeless handler (50 pts):     #{Float.round(us / iterations, 1)}us/req")

# Test with 300 points per request (realistic scrape size)
prom_body_300 = Enum.map_join(1..300, "\n", fn i ->
  "bench_m#{rem(i,20)}{h=\"d#{i}\",e=\"p\"} #{:rand.uniform()*100} #{System.os_time(:millisecond)}"
end)
prom_charlist_300 = String.to_charlist(prom_body_300)

for _ <- 1..100 do
  {:ok, _} = :httpc.request(:post, {~c"http://127.0.0.1:19002/api/v1/import/prometheus", [], ~c"text/plain", prom_charlist_300}, [], [])
end

{us, _} = :timer.tc(fn ->
  for _ <- 1..iterations do
    {:ok, {{_, 204, _}, _, _}} =
      :httpc.request(:post, {~c"http://127.0.0.1:19002/api/v1/import/prometheus", [], ~c"text/plain", prom_charlist_300}, [], [])
  end
end)
IO.puts("  Timeless handler (300 pts):    #{Float.round(us / iterations, 1)}us/req")
IO.puts("  Points/sec at 300 pts/req:     #{Float.round(iterations / (us / 1_000_000) * 300, 0)}")

File.rm_rf!(data_dir)
