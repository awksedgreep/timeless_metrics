defmodule HTTPConcurrencyBench do
  @moduledoc """
  HTTP ingest/query concurrency benchmark using the current embedded store + HTTP child spec.

  Usage:
    mix run bench/http_concurrency.exs
  """

  @store :http_concurrency_bench
  @port 19428
  @duration_ms 5_000
  @device_count 1_000
  @metrics_per_device 20
  @data_dir "/tmp/tm_http_conc_#{System.os_time(:millisecond)}"
  @base_url "http://127.0.0.1:#{@port}"

  def run do
    File.mkdir_p!(@data_dir)

    {:ok, sup} =
      Supervisor.start_link(
        [
          {TimelessMetrics,
           name: @store, data_dir: @data_dir, self_monitor: false, scraping: false},
          {TimelessMetrics.HTTP, store: @store, port: @port},
          {Finch, name: :BenchFinch, pools: %{default: [size: 256, count: 1]}}
        ],
        strategy: :one_for_one
      )

    try do
      wait_until_ready!()
      print_header()
      seed_store!()

      write_payloads = prometheus_payloads()
      json_payloads = json_payloads()
      query_urls = query_urls()

      run_matrix(
        "Prometheus text format (POST /api/v1/import/prometheus)",
        "#{@base_url}/api/v1/import/prometheus",
        "text/plain",
        write_payloads,
        query_urls
      )

      run_matrix(
        "JSON line format (POST /api/v1/import)",
        "#{@base_url}/api/v1/import",
        "application/json",
        json_payloads,
        query_urls
      )

      print_query_path_stats()
    after
      Supervisor.stop(sup)
      File.rm_rf!(@data_dir)
    end
  end

  defp print_header do
    IO.puts("=== HTTP Concurrency Stress Test ===\n")
    IO.puts("Store: #{@store}")
    IO.puts("HTTP: #{@base_url}")
    IO.puts("Each step runs #{div(@duration_ms, 1000)}s.\n")
  end

  defp wait_until_ready! do
    Process.sleep(300)
    :ok
  end

  defp seed_store! do
    series_count = @device_count * @metrics_per_device
    now = System.os_time(:second)
    timestamps = Enum.to_list((now - 600)..now//15)

    IO.puts("Seeding #{series_count} series with #{length(timestamps)} points each...")

    1..@device_count
    |> Task.async_stream(
      fn i ->
        host = host_name(i)

        entries =
          for metric_idx <- 1..@metrics_per_device,
              ts <- timestamps do
            {
              "node_metric_#{metric_idx}",
              %{"host" => host, "region" => "us-east", "env" => "prod"},
              :rand.uniform() * 100.0,
              ts
            }
          end

        :ok = TimelessMetrics.write_batch(@store, entries)
      end,
      max_concurrency: System.schedulers_online(),
      ordered: false,
      timeout: :infinity
    )
    |> Enum.each(fn
      {:ok, :ok} -> :ok
      {:exit, reason} -> raise "seed task failed: #{inspect(reason)}"
    end)

    IO.puts("Seeded #{series_count} series.\n")
  end

  defp prometheus_payloads do
    now_ms = System.os_time(:millisecond)

    for i <- 1..@device_count do
      host = host_name(i)

      for metric_idx <- 1..@metrics_per_device, into: "" do
        ~s(node_metric_#{metric_idx}{host="#{host}",region="us-east",env="prod"} #{:rand.uniform() * 100} #{now_ms}\n)
      end
    end
  end

  defp json_payloads do
    ts = System.os_time(:second)

    for i <- 1..@device_count do
      host = host_name(i)

      1..@metrics_per_device
      |> Enum.map(fn metric_idx ->
        Jason.encode!(%{
          "metric" => %{
            "__name__" => "node_metric_#{metric_idx}",
            "host" => host,
            "region" => "us-east",
            "env" => "prod"
          },
          "values" => [:rand.uniform() * 100],
          "timestamps" => [ts]
        })
      end)
      |> Enum.join("\n")
    end
  end

  defp query_urls do
    List.duplicate("#{@base_url}/health/detailed", 1_000)
  end

  defp run_matrix(title, write_url, content_type, payloads, query_urls) do
    IO.puts("=== #{title} ===")

    IO.puts(
      "  #{String.pad_trailing("Concurrency", 20)} Write                                   | Query"
    )

    IO.puts("  #{String.duplicate("-", 100)}")

    for {writers, query_workers} <- [{1, 1}, {4, 4}, {16, 16}, {32, 32}, {64, 64}] do
      run_step(
        "#{writers}W + #{query_workers}Q",
        writers,
        query_workers,
        payloads,
        query_urls,
        write_url,
        content_type
      )
    end

    IO.puts("")
  end

  defp run_step(label, writers, query_workers, payloads, query_urls, write_url, content_type) do
    stop = :atomics.new(1, [])

    write_latencies =
      :ets.new(:write_latencies, [:ordered_set, :public, {:write_concurrency, true}])

    query_latencies =
      :ets.new(:query_latencies, [:ordered_set, :public, {:write_concurrency, true}])

    write_counters = :counters.new(2, [:write_concurrency])
    query_counters = :counters.new(2, [:write_concurrency])
    write_ids = :atomics.new(1, [])
    query_ids = :atomics.new(1, [])

    writer_tasks =
      for writer_idx <- 1..writers do
        Task.async(fn ->
          payload = Enum.at(payloads, rem(writer_idx - 1, length(payloads)))

          write_loop(
            payload,
            content_type,
            write_url,
            stop,
            write_latencies,
            write_ids,
            write_counters
          )
        end)
      end

    query_tasks =
      for _ <- 1..query_workers do
        Task.async(fn ->
          query_loop(query_urls, stop, query_latencies, query_ids, query_counters)
        end)
      end

    started_at = System.monotonic_time(:microsecond)
    Process.sleep(@duration_ms)
    elapsed_us = System.monotonic_time(:microsecond) - started_at
    :atomics.put(stop, 1, 1)

    Task.await_many(writer_tasks ++ query_tasks, 30_000)

    write_count = :counters.get(write_counters, 1)
    write_errors = :counters.get(write_counters, 2)
    query_count = :counters.get(query_counters, 1)
    query_errors = :counters.get(query_counters, 2)
    elapsed_s = elapsed_us / 1_000_000

    write_rps = trunc(write_count / elapsed_s)
    query_rps = trunc(query_count / elapsed_s)
    write_p50 = percentile(ets_values(write_latencies), 50)
    write_p99 = percentile(ets_values(write_latencies), 99)
    query_p50 = percentile(ets_values(query_latencies), 50)
    query_p99 = percentile(ets_values(query_latencies), 99)

    :ets.delete(write_latencies)
    :ets.delete(query_latencies)

    IO.puts(
      "  #{String.pad_trailing(label, 20)}" <>
        " W: #{pad_rate(write_rps)} rps  p50=#{pad_time(write_p50)}  p99=#{pad_time(write_p99)}" <>
        " | Q: #{pad_rate(query_rps)} rps  p50=#{pad_time(query_p50)}  p99=#{pad_time(query_p99)}" <>
        if(write_errors + query_errors > 0,
          do: "  ERR w=#{write_errors} q=#{query_errors}",
          else: ""
        )
    )
  end

  defp write_loop(payload, content_type, write_url, stop, latencies, ids, counters) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      {us, result} =
        :timer.tc(fn ->
          Finch.build(:post, write_url, [{"content-type", content_type}], payload)
          |> Finch.request(:BenchFinch)
        end)

      id = :atomics.add_get(ids, 1, 1)
      :ets.insert(latencies, {id, us})
      :counters.add(counters, 1, 1)

      case result do
        {:ok, %{status: status}} when status < 400 -> :ok
        _ -> :counters.add(counters, 2, 1)
      end

      write_loop(payload, content_type, write_url, stop, latencies, ids, counters)
    end
  end

  defp query_loop(urls, stop, latencies, ids, counters) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      url = Enum.random(urls)

      {us, result} =
        :timer.tc(fn ->
          Finch.build(:get, url) |> Finch.request(:BenchFinch)
        end)

      id = :atomics.add_get(ids, 1, 1)
      :ets.insert(latencies, {id, us})
      :counters.add(counters, 1, 1)

      case result do
        {:ok, %{status: status}} when status < 400 -> :ok
        _ -> :counters.add(counters, 2, 1)
      end

      query_loop(urls, stop, latencies, ids, counters)
    end
  end

  defp print_query_path_stats do
    stats = TimelessMetrics.Stats.snapshot(@store)
    fast = stats.query_fast_path
    slow = stats.query_slow_path
    total = fast + slow
    rate = if total > 0, do: Float.round(fast / total * 100, 1), else: 0.0

    IO.puts("Fast path: #{fast}  Slow path: #{slow}  Hit rate: #{rate}%")
  end

  defp host_name(i) do
    "device_#{String.pad_leading(Integer.to_string(i), 6, "0")}"
  end

  defp ets_values(table), do: :ets.tab2list(table) |> Enum.map(fn {_, us} -> us end)

  defp percentile([], _p), do: 0

  defp percentile(values, p) do
    sorted = Enum.sort(values)
    index = max(0, trunc(Float.ceil(length(sorted) * p / 100) - 1))
    Enum.at(sorted, index)
  end

  defp pad_rate(n) when n >= 1_000_000,
    do: String.pad_leading("#{Float.round(n / 1_000_000, 1)}M", 7)

  defp pad_rate(n) when n >= 1_000,
    do: String.pad_leading("#{Float.round(n / 1_000, 1)}K", 7)

  defp pad_rate(n), do: String.pad_leading("#{n}", 7)

  defp pad_time(us) when us >= 1_000_000,
    do: String.pad_leading("#{Float.round(us / 1_000_000, 2)}s", 8)

  defp pad_time(us) when us >= 1_000,
    do: String.pad_leading("#{Float.round(us / 1_000, 2)}ms", 8)

  defp pad_time(us), do: String.pad_leading("#{us}us", 8)
end

HTTPConcurrencyBench.run()
