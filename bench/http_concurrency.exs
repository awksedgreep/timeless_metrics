# Benchmark: HTTP latency under increasing concurrency
#
# Ramps up concurrent writers + query workers to find where degradation begins.
# Uses Finch directly (not Req) for minimal client overhead.
#
# Usage:
#   mix run bench/http_concurrency.exs

data_dir = Path.join(System.tmp_dir!(), "tm_conc_#{System.unique_integer([:positive])}")
File.mkdir_p!(data_dir)

{:ok, _} =
  TimelessMetrics.Supervisor.start_link(
    name: :bench_store,
    data_dir: data_dir,
    self_monitor: false,
    scraping: false
  )

# Start HTTP server on a separate port pointed at bench_store
:persistent_term.put({TimelessMetrics.HTTP, :config}, {:bench_store, nil})
{:ok, _} = Rocket.start_link(port: 9428, handler: TimelessMetrics.HTTP, max_body: 10 * 1024 * 1024)

Process.sleep(300)

IO.puts("=== HTTP Concurrency Stress Test ===\n")

# Pre-seed series
device_count = 1000
metrics_per_device = 20
series_count = device_count * metrics_per_device

IO.puts("Seeding #{series_count} series...")
manager = :bench_store_actor_manager

for i <- 1..device_count do
  host = "device_#{String.pad_leading(Integer.to_string(i), 6, "0")}"

  for m <- 1..metrics_per_device do
    metric = "node_metric_#{m}"
    labels = %{"host" => host, "region" => "us-east", "env" => "prod"}
    {_id, pid} = TimelessMetrics.Actor.SeriesManager.get_or_start(manager, metric, labels)
    now = System.os_time(:second)
    batch = for t <- (now - 600)..now//15, do: {t, :rand.uniform() * 100.0}
    send(pid, {:write_batch, batch})
  end
end

Process.sleep(2000)
IO.puts("Seeded #{series_count} series.\n")

# Build Finch pool
Finch.start_link(name: :BenchFinch, pools: %{default: [size: 256, count: 1]})

# Pre-build payloads
now_ms = System.os_time(:millisecond)

write_payloads =
  for i <- 1..device_count do
    host = "device_#{String.pad_leading(Integer.to_string(i), 6, "0")}"

    body =
      for m <- 1..metrics_per_device, into: "" do
        ~s(node_metric_#{m}{host="#{host}",region="us-east",env="prod"} #{:rand.uniform() * 100} #{now_ms}\n)
      end

    body
  end

# JSON import payloads (same format as bench_http.exs)
json_write_payloads =
  for i <- 1..device_count do
    host = "device_#{String.pad_leading(Integer.to_string(i), 6, "0")}"
    ts = System.os_time(:second)

    lines =
      for m <- 1..metrics_per_device do
        ~s({"metric":{"__name__":"node_metric_#{m}","host":"#{host}","env":"prod"},"values":[#{:rand.uniform() * 100}],"timestamps":[#{ts}]})
      end

    Enum.join(lines, "\n")
  end

query_urls =
  for _ <- 1..1000 do
    i = :rand.uniform(device_count)
    m = :rand.uniform(metrics_per_device)
    host = "device_#{String.pad_leading(Integer.to_string(i), 6, "0")}"
    now = System.os_time(:second)
    query = URI.encode_www_form(~s(node_metric_#{m}{host="#{host}"}))
    "http://127.0.0.1:9428/api/v1/query_range?query=#{query}&start=#{now - 3600}&end=#{now}&step=60"
  end

defmodule ConcBench do
  @duration_ms 5_000

  def run_step(label, writers, query_workers, write_payloads, query_urls, write_url \\ "http://127.0.0.1:9428/api/v1/import/prometheus") do
    stop = :atomics.new(1, [])
    w_lat = :ets.new(:wl, [:ordered_set, :public, {:write_concurrency, true}])
    q_lat = :ets.new(:ql, [:ordered_set, :public, {:write_concurrency, true}])
    w_ctr = :counters.new(2, [:write_concurrency])  # 1=count 2=errors
    q_ctr = :counters.new(2, [:write_concurrency])
    w_id = :atomics.new(1, [])
    q_id = :atomics.new(1, [])

    w_tasks =
      for w <- 1..writers do
        Task.async(fn ->
          payload = Enum.at(write_payloads, rem(w - 1, length(write_payloads)))
          write_loop(payload, write_url, stop, w_lat, w_id, w_ctr)
        end)
      end

    q_tasks =
      for _ <- 1..query_workers do
        Task.async(fn ->
          query_loop(query_urls, stop, q_lat, q_id, q_ctr)
        end)
      end

    t0 = System.monotonic_time(:microsecond)
    Process.sleep(@duration_ms)
    elapsed_us = System.monotonic_time(:microsecond) - t0
    :atomics.put(stop, 1, 1)

    Task.await_many(w_tasks ++ q_tasks, 30_000)

    elapsed_s = elapsed_us / 1_000_000
    w_count = :counters.get(w_ctr, 1)
    w_errs = :counters.get(w_ctr, 2)
    q_count = :counters.get(q_ctr, 1)
    q_errs = :counters.get(q_ctr, 2)
    w_lats = ets_values(w_lat)
    q_lats = ets_values(q_lat)

    :ets.delete(w_lat)
    :ets.delete(q_lat)

    w_rps = trunc(w_count / elapsed_s)
    q_rps = trunc(q_count / elapsed_s)
    w_p50 = percentile(w_lats, 50)
    w_p99 = percentile(w_lats, 99)
    q_p50 = percentile(q_lats, 50)
    q_p99 = percentile(q_lats, 99)

    IO.puts(
      "  #{String.pad_trailing(label, 20)}" <>
        " W: #{pad(w_rps)} rps  p50=#{pad_us(w_p50)}  p99=#{pad_us(w_p99)}" <>
        " | Q: #{pad(q_rps)} rps  p50=#{pad_us(q_p50)}  p99=#{pad_us(q_p99)}" <>
        if(w_errs + q_errs > 0, do: "  ERR w=#{w_errs} q=#{q_errs}", else: "")
    )
  end

  defp write_loop(payload, url, stop, ets, wid, ctr) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      {us, result} =
        :timer.tc(fn ->
          Finch.build(:post, url, [{"content-type", "text/plain"}], payload)
          |> Finch.request(:BenchFinch)
        end)

      id = :atomics.add_get(wid, 1, 1)
      :ets.insert(ets, {id, us})
      :counters.add(ctr, 1, 1)

      case result do
        {:ok, %{status: s}} when s < 400 -> :ok
        _ -> :counters.add(ctr, 2, 1)
      end

      write_loop(payload, url, stop, ets, wid, ctr)
    end
  end

  defp query_loop(urls, stop, ets, qid, ctr) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      url = Enum.random(urls)

      {us, result} =
        :timer.tc(fn ->
          Finch.build(:get, url) |> Finch.request(:BenchFinch)
        end)

      id = :atomics.add_get(qid, 1, 1)
      :ets.insert(ets, {id, us})
      :counters.add(ctr, 1, 1)

      case result do
        {:ok, %{status: s}} when s < 400 -> :ok
        _ -> :counters.add(ctr, 2, 1)
      end

      query_loop(urls, stop, ets, qid, ctr)
    end
  end

  defp ets_values(tab), do: :ets.tab2list(tab) |> Enum.map(fn {_, us} -> us end)

  defp percentile([], _), do: 0
  defp percentile(values, p) do
    sorted = Enum.sort(values)
    k = max(0, trunc(Float.ceil(length(sorted) * p / 100) - 1))
    Enum.at(sorted, k)
  end

  defp pad(n) when n >= 1_000_000, do: String.pad_leading("#{Float.round(n / 1_000_000, 1)}M", 7)
  defp pad(n) when n >= 1_000, do: String.pad_leading("#{Float.round(n / 1_000, 1)}K", 7)
  defp pad(n), do: String.pad_leading("#{n}", 7)

  defp pad_us(us) when us >= 1_000_000, do: String.pad_leading("#{Float.round(us / 1_000_000, 2)}s", 8)
  defp pad_us(us) when us >= 1_000, do: String.pad_leading("#{Float.round(us / 1_000, 2)}ms", 8)
  defp pad_us(us), do: String.pad_leading("#{us}us", 8)
end

IO.puts("Each step runs 5s. Ramping concurrency...\n")

IO.puts("=== Prometheus text format (POST /api/v1/import/prometheus) ===")
IO.puts("  #{String.pad_trailing("Concurrency", 20)} Write                                   | Query")
IO.puts("  #{String.duplicate("-", 100)}")

for {w, q} <- [{1, 1}, {4, 4}, {16, 16}, {32, 32}, {64, 64}] do
  ConcBench.run_step("#{w}W + #{q}Q", w, q, write_payloads, query_urls)
end

IO.puts("\n=== JSON line format (POST /api/v1/import) — same as bench_http.exs ===")
IO.puts("  #{String.pad_trailing("Concurrency", 20)} Write                                   | Query")
IO.puts("  #{String.duplicate("-", 100)}")

json_url = "http://127.0.0.1:9428/api/v1/import"
for {w, q} <- [{1, 1}, {4, 4}, {16, 16}, {32, 32}, {64, 64}] do
  ConcBench.run_step("#{w}W + #{q}Q", w, q, json_write_payloads, query_urls, json_url)
end

# Print fast/slow path stats
stats = TimelessMetrics.Stats.snapshot(:bench_store)
fast = stats.query_fast_path
slow = stats.query_slow_path
total = fast + slow
rate = if total > 0, do: Float.round(fast / total * 100, 1), else: 0.0
IO.puts("\nFast path: #{fast}  Slow path: #{slow}  Hit rate: #{rate}%")

File.rm_rf!(data_dir)
