defmodule RustSeriesBench do
  @moduledoc """
  Focused Rust-engine benchmark for series resolution and first-write cost.

  Measures:
  - resolving a large set of unseen series
  - resolving the same series again through the cache
  - writing one point for a large set of unseen series
  - flush cost after the first-write burst

  Usage:
    mix run bench/rust_series_bench.exs
    mix run bench/rust_series_bench.exs --memory
    mix run bench/rust_series_bench.exs --series 20000
  """

  alias TimelessMetrics.DataGenerator

  @default_series 20_000

  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        switches: [
          memory: :boolean,
          series: :integer
        ]
      )

    memory_only = Keyword.get(opts, :memory, false)
    series_count = Keyword.get(opts, :series, @default_series)

    mode_label = if memory_only, do: "MEMORY-ONLY", else: "DISK"
    data_dir = "/tmp/timeless_rust_series_bench_#{System.os_time(:millisecond)}"
    store = :"rust_series_bench_#{System.unique_integer([:positive])}"
    metric = "series_hot_metric"
    now = System.os_time(:second)
    series = build_series(metric, series_count)

    store_opts =
      if memory_only do
        [name: store, data_dir: data_dir, mode: :memory, self_monitor: false]
      else
        [name: store, data_dir: data_dir, self_monitor: false]
      end

    {:ok, sup} =
      Supervisor.start_link(
        [{TimelessMetrics, store_opts}],
        strategy: :one_for_one
      )

    try do
      IO.puts("")
      IO.puts("=============================================")
      IO.puts("  TimelessMetrics Rust Series Benchmark (#{mode_label})")
      IO.puts("  Schedulers: #{System.schedulers_online()}")
      IO.puts("  Series: #{format_number(series_count)}")
      IO.puts("=============================================")
      IO.puts("")

      {resolve_new_us, resolved_ids} =
        :timer.tc(fn ->
          Enum.map(series, fn {metric_name, labels, _idx} ->
            {:ok, series_id} = TimelessMetrics.resolve_series(store, metric_name, labels)
            series_id
          end)
        end)

      {resolve_cached_us, cached_ids} =
        :timer.tc(fn ->
          Enum.map(series, fn {metric_name, labels, _idx} ->
            {:ok, series_id} = TimelessMetrics.resolve_series(store, metric_name, labels)
            series_id
          end)
        end)

      first_write_entries =
        Enum.map(series, fn {metric_name, labels, idx} ->
          {metric_name, labels, DataGenerator.value(metric_name, now * 1000, idx), now}
        end)

      fresh_store = :"#{store}_fresh"
      fresh_data_dir = "#{data_dir}_fresh"

      fresh_store_opts =
        if memory_only do
          [name: fresh_store, data_dir: fresh_data_dir, mode: :memory, self_monitor: false]
        else
          [name: fresh_store, data_dir: fresh_data_dir, self_monitor: false]
        end

      {:ok, fresh_sup} =
        Supervisor.start_link(
          [{TimelessMetrics, fresh_store_opts}],
          strategy: :one_for_one
        )

      {first_write_us, :ok} =
        :timer.tc(fn ->
          TimelessMetrics.write_batch(fresh_store, first_write_entries)
        end)

      {flush_us, :ok} =
        :timer.tc(fn ->
          TimelessMetrics.flush(fresh_store)
        end)

      IO.puts(
        "Resolve new series:            #{format_number(rate(series_count, resolve_new_us))}/sec  (#{div(resolve_new_us, 1000)}ms)"
      )

      IO.puts(
        "Resolve cached series:         #{format_number(rate(series_count, resolve_cached_us))}/sec  (#{div(resolve_cached_us, 1000)}ms)"
      )

      IO.puts(
        "First write for new series:    #{format_number(rate(series_count, first_write_us))}/sec  (#{div(first_write_us, 1000)}ms)"
      )

      IO.puts("Flush after first write:       #{div(flush_us, 1000)}ms")
      IO.puts("Resolved ids stable:           #{resolved_ids == cached_ids}")
      IO.puts("")

      Supervisor.stop(fresh_sup)

      if not memory_only do
        File.rm_rf!(fresh_data_dir)
      end
    after
      Supervisor.stop(sup)
      if not memory_only, do: File.rm_rf!(data_dir)
    end
  end

  defp build_series(metric, series_count) do
    for idx <- 1..series_count do
      labels = %{
        "host" => "device_#{idx}",
        "env" => "prod",
        "region" => if(rem(idx, 2) == 0, do: "us-east", else: "us-west"),
        "service" => "svc_#{rem(idx, 32)}"
      }

      {metric, labels, idx}
    end
  end

  defp rate(count, elapsed_us) when elapsed_us > 0, do: trunc(count * 1_000_000 / elapsed_us)

  defp format_number(n) when is_integer(n) do
    n
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/.{3}(?=.)/, "\\0,")
    |> String.reverse()
  end
end

RustSeriesBench.run(System.argv())
