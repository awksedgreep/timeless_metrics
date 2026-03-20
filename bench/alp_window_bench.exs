# Benchmark ALP compression ratio at different window sizes
# Run with: mix run bench/alp_window_bench.exs

IO.puts("=== ALP Compression Ratio vs Window Size ===\n")

# Generate realistic time-series data: one point every 10 seconds
# Simulating a typical Phoenix telemetry metric (request duration avg)
points_per_hour = 360
total_hours = 25
total_points = points_per_hour * total_hours

base_ts = System.os_time(:second) - total_hours * 3600

# Generate several realistic series with different patterns
series = %{
  "request_duration_avg" => fn i ->
    # Slowly varying with noise: 50-150ms range, sinusoidal daily pattern
    base = 80.0 + 30.0 * :math.sin(i / 180.0)
    base + (:rand.uniform() * 20.0 - 10.0)
  end,
  "memory_mb" => fn i ->
    # Slowly climbing with sawtooth (GC drops)
    base = 256.0 + i * 0.05
    if rem(i, 120) == 0, do: base * 0.7, else: base + (:rand.uniform() * 5.0 - 2.5)
  end,
  "cpu_percent" => fn i ->
    # Spiky: mostly low, occasional bursts
    base = 5.0 + :rand.uniform() * 3.0
    if rem(i, 50) < 3, do: 40.0 + :rand.uniform() * 50.0, else: base
  end,
  "queue_depth" => fn i ->
    # Integer-like values, 0-20 range
    (5.0 + 3.0 * :math.sin(i / 60.0) + :rand.uniform() * 4.0) |> Float.round(0)
  end,
  "requests_per_sec" => fn i ->
    # Follows daily pattern, high variance
    base = 100.0 + 80.0 * :math.sin(i / 180.0)
    max(0.0, base + (:rand.uniform() * 40.0 - 20.0))
  end
}

all_points =
  Enum.map(series, fn {name, gen_fn} ->
    points =
      Enum.map(0..(total_points - 1), fn i ->
        ts = base_ts + i * 10
        {ts, gen_fn.(i)}
      end)

    {name, points}
  end)

# Window sizes to test
windows = [
  {"10min", 60},
  {"30min", 180},
  {"1hr", 360},
  {"2hr", 720},
  {"4hr", 1440},
  {"8hr", 2880},
  {"12hr", 4320},
  {"24hr", 8640}
]

# Also test zstd-only (stage 2 format) for comparison
IO.puts(
  String.pad_trailing("Window", 8) <>
    String.pad_trailing("Points", 8) <>
    Enum.map_join(series, "", fn {name, _} ->
      String.pad_leading(String.slice(name, 0, 12), 14)
    end) <>
    String.pad_leading("Avg Ratio", 12) <>
    String.pad_leading("zstd-only", 12)
)

IO.puts(String.duplicate("-", 8 + 8 + length(Map.keys(series)) * 14 + 12 + 12))

for {label, point_count} <- windows do
  ratios =
    Enum.map(all_points, fn {_name, points} ->
      chunk = Enum.take(points, point_count)
      raw_size = byte_size(:erlang.term_to_binary(chunk))

      alp_ratio =
        case ExAlp.compress(chunk, compression: :zstd, compression_level: 9) do
          {:ok, blob} -> raw_size / byte_size(blob)
          _ -> 0.0
        end

      alp_ratio
    end)

  zstd_ratios =
    Enum.map(all_points, fn {_name, points} ->
      chunk = Enum.take(points, point_count)
      raw = :erlang.term_to_binary(chunk)
      compressed = :ezstd.compress(raw, 1)
      byte_size(raw) / byte_size(compressed)
    end)

  avg_alp = Enum.sum(ratios) / length(ratios)
  avg_zstd = Enum.sum(zstd_ratios) / length(zstd_ratios)

  ratio_strs =
    Enum.map_join(ratios, "", fn r ->
      String.pad_leading("#{Float.round(r, 1)}x", 14)
    end)

  IO.puts(
    String.pad_trailing(label, 8) <>
      String.pad_trailing("#{point_count}", 8) <>
      ratio_strs <>
      String.pad_leading("#{Float.round(avg_alp, 1)}x", 12) <>
      String.pad_leading("#{Float.round(avg_zstd, 1)}x", 12)
  )
end

IO.puts("")

# Also show absolute sizes for the 1hr and 4hr windows to see the actual difference
IO.puts("=== Absolute sizes (bytes per series) ===\n")

for {label, point_count} <- [{"1hr", 360}, {"4hr", 1440}, {"12hr", 4320}, {"24hr", 8640}] do
  IO.puts("#{label} (#{point_count} points):")

  Enum.each(all_points, fn {name, points} ->
    chunk = Enum.take(points, point_count)
    raw_size = byte_size(:erlang.term_to_binary(chunk))

    zstd_size =
      chunk
      |> :erlang.term_to_binary()
      |> :ezstd.compress(1)
      |> byte_size()

    alp_size =
      case ExAlp.compress(chunk, compression: :zstd, compression_level: 9) do
        {:ok, blob} -> byte_size(blob)
        _ -> 0
      end

    IO.puts(
      "  #{String.pad_trailing(name, 25)} raw=#{String.pad_leading("#{raw_size}", 7)}  " <>
        "zstd=#{String.pad_leading("#{zstd_size}", 6)}  " <>
        "ALP=#{String.pad_leading("#{alp_size}", 6)}  " <>
        "ALP/zstd=#{Float.round(alp_size / max(zstd_size, 1), 2)}"
    )
  end)

  IO.puts("")
end
