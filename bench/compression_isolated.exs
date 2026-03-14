# Isolated compression benchmark — no HTTP, no ETS, just compress/decompress
# Tests GorillaStream with zstd vs openzl at various levels using realistic data
#
# Run: mix run bench/compression_isolated.exs

defmodule RealisticData do
  @doc "Generate realistic telemetry points for a gauge metric (CPU-like, oscillates 30-70%)"
  def gauge(count, base \\ 45.0) do
    Enum.map(0..(count - 1), fn i ->
      ts = 1_700_000_000 + i * 15
      drift = :math.sin(i / 50) * 15 + :math.sin(i / 200) * 5
      noise = (:rand.uniform() - 0.5) * 2
      {ts, Float.round(base + drift + noise, 2)}
    end)
  end

  @doc "Generate realistic monotonic counter (bytes_total-like, always increasing)"
  def counter(count, base \\ 1_000_000_000.0) do
    Enum.scan(0..(count - 1), {1_700_000_000, base}, fn i, {_ts, prev} ->
      ts = 1_700_000_000 + i * 15
      increment = 1000 + :rand.uniform(5000)
      {ts, Float.round(prev + increment / 1, 0)}
    end)
  end

  @doc "Generate random data (worst case for compression)"
  def random(count) do
    Enum.map(0..(count - 1), fn i ->
      ts = 1_700_000_000 + i * 15
      {ts, Float.round(:rand.uniform() * 100, 2)}
    end)
  end
end

defmodule CompressionTest do
  def run_test(points, label, compression, level) do
    count = length(points)

    # Compress
    opts = case compression do
      :zstd -> [compression: :zstd, compression_level: level]
      :openzl -> [compression: :openzl, compression_level: level]
      :none -> [compression: :none]
    end

    {compress_us, result} = :timer.tc(fn -> GorillaStream.compress(points, opts) end)

    case result do
      {:ok, blob} ->
        blob_size = byte_size(blob)
        raw_size = count * 16  # 8 bytes ts + 8 bytes float
        ratio = Float.round(raw_size / blob_size, 1)
        bytes_per_pt = Float.round(blob_size / count, 3)
        compress_rate = Float.round(count / (compress_us / 1_000_000), 0)

        # Decompress
        {decompress_us, {:ok, _decoded}} = :timer.tc(fn ->
          GorillaStream.decompress(blob, compression: elem(List.keyfind(opts, :compression, 0), 1))
        end)
        decompress_rate = Float.round(count / (decompress_us / 1_000_000), 0)

        IO.puts(
          String.pad_trailing(label, 35) <>
          " | #{String.pad_leading(Integer.to_string(count), 7)} pts" <>
          " | #{String.pad_leading(fmt_bytes(blob_size), 10)}" <>
          " | #{String.pad_leading(Float.to_string(bytes_per_pt), 7)} B/pt" <>
          " | #{String.pad_leading(Float.to_string(ratio), 5)}x" <>
          " | compress: #{String.pad_leading(fmt_rate(compress_rate), 8)}/s" <>
          " | decompress: #{String.pad_leading(fmt_rate(decompress_rate), 8)}/s"
        )

      {:error, reason} ->
        IO.puts("#{label}: ERROR #{inspect(reason)}")
    end
  end

  defp fmt_bytes(n) when n >= 1_048_576, do: "#{Float.round(n / 1_048_576, 1)}MB"
  defp fmt_bytes(n) when n >= 1024, do: "#{Float.round(n / 1024, 1)}KB"
  defp fmt_bytes(n), do: "#{n}B"

  defp fmt_rate(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)}M"
  defp fmt_rate(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  defp fmt_rate(n), do: "#{Float.round(n, 0)}"
end

IO.puts("=== Isolated Compression Benchmark ===")
IO.puts("GorillaStream #{Application.spec(:gorilla_stream, :vsn)}, ezstd #{Application.spec(:ezstd, :vsn)}, ex_openzl #{Application.spec(:ex_openzl, :vsn)}\n")

for {data_label, points} <- [
  {"gauge (CPU-like)", RealisticData.gauge(10_000)},
  {"counter (bytes_total)", RealisticData.counter(10_000)},
  {"random (worst case)", RealisticData.random(10_000)}
] do
  IO.puts("--- #{data_label} ---")

  # No compression (gorilla only)
  CompressionTest.run_test(points, "gorilla only", :none, 0)

  # Zstd levels
  for level <- [1, 2, 3, 5, 9, 15, 19] do
    CompressionTest.run_test(points, "gorilla + zstd level #{level}", :zstd, level)
  end

  # OpenZL levels
  for level <- [1, 3, 6, 9] do
    CompressionTest.run_test(points, "gorilla + openzl level #{level}", :openzl, level)
  end

  IO.puts("")
end

# Larger block sizes with best compression
IO.puts("--- Block size scaling (gauge data, zstd level 2) ---")
for size <- [1_000, 5_000, 10_000, 50_000, 100_000, 500_000] do
  points = RealisticData.gauge(size)
  CompressionTest.run_test(points, "#{div(size, 1000)}K points", :zstd, 2)
end

IO.puts("\nDone.")
