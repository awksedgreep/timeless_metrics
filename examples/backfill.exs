# Backfill historical ISP data for UI development
#
# Generates 7 days of realistic metric data for 50 devices:
#   - cpu_usage, mem_usage, if_throughput_mbps, snr_tenth_dbmv, snmp_uptime
#   - Includes daily/weekly patterns, traffic spikes, device reboots, SNR degradation
#
# Usage:
#   elixir examples/backfill.exs [url] [days]
#
# Defaults: http://localhost:4001 , 7 days

{url, days} =
  case System.argv() do
    [url, days] -> {url, String.to_integer(days)}
    [url] -> {url, 7}
    _ -> {"http://localhost:4001", 7}
  end

import_url = "#{url}/api/v1/import"

devices = 50
metrics_per_device = 5
interval = 300  # 5-minute polling
now = System.os_time(:second)
start = now - days * 86_400
total_ticks = div(days * 86_400, interval)
points_per_tick = devices * metrics_per_device

IO.puts("Backfilling #{days} days of data for #{devices} devices")
IO.puts("  #{total_ticks} ticks × #{points_per_tick} points = #{total_ticks * points_per_tick} total points")
IO.puts("  Target: #{import_url}")
IO.puts("")

# Pre-generate device reboot times (2-3 reboots per device over the period)
reboots =
  for i <- 1..devices, into: %{} do
    count = Enum.random(2..3)
    times = for _ <- 1..count, do: start + Enum.random(0..(days * 86_400))
    {i, Enum.sort(times)}
  end

# Pre-generate SNR degradation events (affects 10% of devices for 1-4 hours)
snr_events =
  for i <- 1..devices, :rand.uniform() < 0.1, into: %{} do
    event_start = start + Enum.random(0..(days * 86_400 - 14_400))
    duration = Enum.random(3_600..14_400)
    {i, {event_start, event_start + duration}}
  end

# Pre-generate traffic spike windows (evening peak 18:00-22:00 daily boost)
# Plus 2-3 random anomalous spikes
spike_times =
  for _ <- 1..Enum.random(2..3) do
    spike_start = start + Enum.random(0..(days * 86_400 - 7_200))
    {spike_start, spike_start + Enum.random(1_800..7_200)}
  end

# Process in batches of 100 ticks to avoid massive HTTP bodies
batch_size = 100

0..(total_ticks - 1)
|> Enum.chunk_every(batch_size)
|> Enum.with_index(1)
|> Enum.each(fn {tick_batch, batch_num} ->
  lines =
    Enum.flat_map(tick_batch, fn tick_idx ->
      ts = start + tick_idx * interval
      hour_of_day = rem(div(ts, 3600), 24)
      day_of_week = rem(div(ts, 86_400), 7)

      # Daily pattern: higher during business hours (8-18), lower at night
      daily_factor = if hour_of_day >= 8 and hour_of_day <= 18, do: 1.3, else: 0.7

      # Weekly pattern: weekdays busier than weekends
      weekly_factor = if day_of_week < 5, do: 1.1, else: 0.85

      # Evening peak boost
      evening_factor = if hour_of_day >= 18 and hour_of_day <= 22, do: 1.4, else: 1.0

      # Random spike active?
      spike_active = Enum.any?(spike_times, fn {s, e} -> ts >= s and ts <= e end)
      spike_factor = if spike_active, do: 1.8, else: 1.0

      for i <- 1..devices do
        device = :io_lib.format("device-~3..0B", [i]) |> IO.iodata_to_binary()
        base_load = 30 + :rand.uniform(20)

        # CPU: 15-85% with daily/weekly patterns
        cpu = base_load * daily_factor * weekly_factor + :rand.uniform() * 10
        cpu = min(cpu, 99.0)

        # Memory: 40-90%, slowly climbs then drops on reboot
        rebooted_recently = Enum.any?(reboots[i] || [], fn rt -> ts >= rt and ts < rt + 600 end)
        mem_base = if rebooted_recently, do: 40.0, else: 55 + :math.sin(ts / 86_400 + i) * 15
        mem = mem_base + :rand.uniform() * 8

        # Throughput: 50-800 Mbps with evening peak and spikes
        bw = (200 + 150 * :math.sin(ts / 43_200 + i * 0.3)) * evening_factor * spike_factor * weekly_factor
        bw = bw + :rand.uniform() * 80
        bw = max(bw, 10.0)

        # SNR: 340-400 normally, drops to 200-280 during degradation events
        snr_degraded = case Map.get(snr_events, i) do
          {es, ee} when ts >= es and ts <= ee -> true
          _ -> false
        end
        snr = if snr_degraded, do: 200 + :rand.uniform(80), else: 340 + :rand.uniform(60)

        # Uptime: monotonic, resets on reboot
        last_reboot = Enum.filter(reboots[i] || [], &(&1 <= ts)) |> List.last() || (start - i * 86_400)
        uptime = ts - last_reboot

        [
          Jason.encode!(%{metric: %{__name__: "cpu_usage", device: device, region: "east"}, values: [Float.round(cpu, 1)], timestamps: [ts]}),
          Jason.encode!(%{metric: %{__name__: "mem_usage", device: device, region: "east"}, values: [Float.round(mem, 1)], timestamps: [ts]}),
          Jason.encode!(%{metric: %{__name__: "if_throughput_mbps", device: device, interface: "ae0", region: "east"}, values: [round(bw)], timestamps: [ts]}),
          Jason.encode!(%{metric: %{__name__: "snr_tenth_dbmv", device: device, interface: "cm0", region: "east"}, values: [snr], timestamps: [ts]}),
          Jason.encode!(%{metric: %{__name__: "snmp_uptime", device: device, region: "east"}, values: [uptime], timestamps: [ts]})
        ]
      end
    end)

  body = Enum.join(lines, "\n")

  case :httpc.request(
         :post,
         {String.to_charlist(import_url), [], ~c"application/json", body},
         [{:timeout, 30_000}],
         []
       ) do
    {:ok, {{_, 204, _}, _, _}} ->
      :ok

    {:ok, {{_, status, _}, _, resp_body}} ->
      IO.puts("  Warning: batch #{batch_num} got HTTP #{status}: #{resp_body}")

    {:error, reason} ->
      IO.puts("  Error: batch #{batch_num} failed: #{inspect(reason)}")
  end

  total_batches = div(total_ticks + batch_size - 1, batch_size)
  pct = round(batch_num / total_batches * 100)
  pts = length(tick_batch) * points_per_tick

  if rem(batch_num, 10) == 0 or batch_num == total_batches do
    IO.puts("  [#{pct}%] batch #{batch_num}/#{total_batches} — #{pts} points")
  end
end)

IO.puts("")
IO.puts("Done! Checking health...")

case :httpc.request(:get, {String.to_charlist("#{url}/health"), []}, [{:timeout, 5_000}], []) do
  {:ok, {{_, 200, _}, _, body}} ->
    IO.puts("  #{body}")

  _ ->
    IO.puts("  Could not reach health endpoint")
end
