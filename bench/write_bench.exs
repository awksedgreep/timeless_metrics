defmodule WriteBench do
  @data_dir "/tmp/timeless_bench_#{System.os_time(:millisecond)}"

  def run do
    IO.puts("\n=============================================")
    IO.puts("  Timeless Write Throughput Benchmark")
    IO.puts("=============================================\n")

    for scale <- [1_000, 10_000, 100_000, 1_000_000] do
      run_scale(scale)
    end

    IO.puts("--- Compression Analysis ---\n")
    run_compression_analysis()
  end

  defp run_scale(point_count) do
    data_dir = "#{@data_dir}_#{point_count}"

    {:ok, sup} =
      Supervisor.start_link(
        [{Timeless, name: :bench, data_dir: data_dir, buffer_shards: System.schedulers_online()}],
        strategy: :one_for_one
      )

    now = System.os_time(:second)
    series_count = max(div(point_count, 100), 10)
    points_per_series = div(point_count, series_count)

    label = format_number(point_count)
    IO.puts("--- #{label} points (#{series_count} series × #{points_per_series} points) ---\n")

    # Single writes
    {single_us, _} =
      :timer.tc(fn ->
        for s <- 1..series_count, i <- 0..(points_per_series - 1) do
          Timeless.write(:bench, "metric", %{"id" => "#{s}"}, :rand.uniform() * 100,
            timestamp: now - point_count + s * points_per_series + i
          )
        end
      end)

    single_rate = trunc(point_count / (single_us / 1_000_000))
    IO.puts("  Write (single):  #{format_number(single_rate)}/sec  (#{div(single_us, 1000)}ms)")

    # Flush
    {flush_us, _} = :timer.tc(fn -> Timeless.flush(:bench) end)
    IO.puts("  Flush + compress: #{div(flush_us, 1000)}ms")

    # Info
    info = Timeless.info(:bench)
    IO.puts("  Segments: #{info.segment_count}")
    IO.puts("  Compressed: #{format_bytes(info.raw_compressed_bytes)}")
    IO.puts("  Bytes/point: #{info.bytes_per_point}")
    IO.puts("  DB size: #{format_bytes(info.storage_bytes)}")

    # Query benchmark
    series_id_to_query = Timeless.SeriesRegistry.get_or_create(:"bench_registry", "metric", %{"id" => "1"})

    {query_us, {:ok, points}} =
      :timer.tc(fn ->
        Timeless.query(:bench, "metric", %{"id" => "1"}, from: 0, to: now + point_count)
      end)

    IO.puts("  Query #{length(points)} points: #{div(query_us, 1000)}ms")
    IO.puts("")

    Supervisor.stop(sup)
    :persistent_term.erase({Timeless, :bench, :schema})
    File.rm_rf!(data_dir)
  end

  defp run_compression_analysis do
    data_dir = "#{@data_dir}_compression"

    {:ok, sup} =
      Supervisor.start_link(
        [{Timeless, name: :cmp, data_dir: data_dir, buffer_shards: 1}],
        strategy: :one_for_one
      )

    now = System.os_time(:second)
    n = 10_000

    patterns = [
      {"Constant (42.0)", fn _i -> 42.0 end},
      {"Counter (monotonic)", fn i -> i * 1.0 end},
      {"Sine wave", fn i -> :math.sin(i / 100 * :math.pi()) * 50 + 50 end},
      {"Random (uniform)", fn _i -> :rand.uniform() * 100 end},
      {"Spiky (mostly flat)", fn i -> if rem(i, 100) == 0, do: 100.0, else: 0.0 end}
    ]

    IO.puts("  #{n} points per pattern:\n")
    IO.puts("  ┌──────────────────────┬────────────┬──────────────┐")
    IO.puts("  │ Pattern              │ Compressed │ Bytes/point  │")
    IO.puts("  ├──────────────────────┼────────────┼──────────────┤")

    Enum.each(patterns, fn {label, gen_fn} ->
      # Write points
      for i <- 0..(n - 1) do
        Timeless.write(:cmp, label, %{"t" => "1"}, gen_fn.(i), timestamp: now - n + i)
      end

      Timeless.flush(:cmp)

      info = Timeless.info(:cmp)
      bytes = info.raw_compressed_bytes
      bpp = Float.round(bytes / n, 2)

      label_fmt = String.pad_trailing(label, 20)
      bytes_fmt = String.pad_leading(format_bytes(bytes), 10)
      bpp_fmt = String.pad_leading("#{bpp}", 12)

      IO.puts("  │ #{label_fmt} │ #{bytes_fmt} │ #{bpp_fmt} │")

      # Reset for next pattern - drop table data
      Timeless.DB.write(:"cmp_db", "DELETE FROM raw_segments", [])
      Timeless.DB.write(:"cmp_db", "DELETE FROM series", [])
    end)

    IO.puts("  └──────────────────────┴────────────┴──────────────┘")
    IO.puts("")

    Supervisor.stop(sup)
    :persistent_term.erase({Timeless, :cmp, :schema})
    File.rm_rf!(data_dir)
  end

  # --- Helpers ---

  defp format_number(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)}M"
  defp format_number(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  defp format_number(n), do: "#{n}"

  defp format_bytes(b) when b >= 1_048_576, do: "#{Float.round(b / 1_048_576, 1)} MB"
  defp format_bytes(b) when b >= 1024, do: "#{Float.round(b / 1024, 1)} KB"
  defp format_bytes(b), do: "#{b} B"
end

WriteBench.run()
