defmodule TimelessMetrics.DataGenerator do
  @moduledoc """
  Realistic telemetry data generators for benchmarks and testing.

  Generates node_exporter-compatible metric names and values that
  produce realistic compression patterns:
  - Gauges: oscillate around a base with drift and noise
  - Counters: monotonically increase with variable increments
  - Constants: rarely change (filesystem size, boot time, etc.)

  ## Usage

      # Generate a list of metric definitions
      metrics = TimelessMetrics.DataGenerator.metrics()

      # Generate a value for a specific metric at a given timestamp
      val = TimelessMetrics.DataGenerator.value("node_load1", ts_ms, device_id)

      # Generate a full scrape payload (all metrics for one device)
      points = TimelessMetrics.DataGenerator.scrape(:metrics, device_id, timestamp)
  """

  @metrics [
    # Gauges — oscillate, good for ALP
    "node_cpu_seconds_total",
    "node_load1",
    "node_load5",
    "node_load15",
    "node_memory_MemAvailable_bytes",
    "node_memory_Buffers_bytes",
    "node_memory_Cached_bytes",
    "node_entropy_avail_bits",
    "node_filefd_allocated",
    "node_procs_running",
    "node_procs_blocked",
    "node_scrape_collector_duration_seconds",
    "node_nf_conntrack_entries",
    # Counters — monotonically increasing
    "node_disk_read_bytes_total",
    "node_disk_written_bytes_total",
    "node_disk_io_time_seconds_total",
    "node_network_receive_bytes_total",
    "node_network_transmit_bytes_total",
    "node_network_receive_errs_total",
    "node_network_transmit_errs_total",
    "node_context_switches_total",
    "node_forks_total",
    "node_intr_total",
    "node_vmstat_pgfault",
    # Constants — rarely change
    "node_memory_MemTotal_bytes",
    "node_filesystem_avail_bytes",
    "node_filesystem_size_bytes",
    "node_boot_time_seconds",
    "node_scrape_collector_success"
  ]

  @doc "Return the list of metric names."
  def metrics, do: @metrics

  @doc "Return the number of metrics per device."
  def metric_count, do: length(@metrics)

  @doc """
  Generate a realistic value for a metric at a given timestamp and device.

  Uses deterministic hashing so the same (metric, ts, device) always
  produces the same value — reproducible benchmarks.
  """
  def value(metric, ts_ms, dev_id) do
    h = :erlang.phash2({ts_ms, dev_id, metric})
    n = h / 4_294_967_295
    dh = :erlang.phash2(dev_id) / 4_294_967_295

    case metric do
      "node_cpu_seconds_total" -> Float.round(1000.0 + ts_ms / 100_000 + dh * 500.0 + n * 10.0, 2)
      "node_memory_MemAvailable_bytes" -> Float.round(2.0e9 + n * 6.0e9 + dh * 2.0e9, 0)
      "node_memory_MemTotal_bytes" -> Float.round(8.0e9 + dh * 8.0e9, 0)
      "node_memory_Buffers_bytes" -> Float.round(1.0e8 + n * 5.0e8, 0)
      "node_memory_Cached_bytes" -> Float.round(5.0e8 + n * 2.0e9, 0)
      "node_filesystem_avail_bytes" -> Float.round(5.0e10 + n * 2.0e11 + dh * 1.0e11, 0)
      "node_filesystem_size_bytes" -> Float.round(5.0e11 + dh * 5.0e11, 0)
      "node_disk_read_bytes_total" -> Float.round(1.0e9 + ts_ms / 1_000 * 1000 + n * 1.0e5, 0)
      "node_disk_written_bytes_total" -> Float.round(2.0e9 + ts_ms / 1_000 * 2000 + n * 2.0e5, 0)
      "node_disk_io_time_seconds_total" -> Float.round(500.0 + ts_ms / 1.0e6 + n * 5.0, 2)
      "node_network_receive_bytes_total" -> Float.round(5.0e9 + ts_ms / 1_000 * 5000 + n * 5.0e5, 0)
      "node_network_transmit_bytes_total" -> Float.round(3.0e9 + ts_ms / 1_000 * 3000 + n * 3.0e5, 0)
      "node_network_receive_errs_total" -> Float.round(n * 100.0, 0)
      "node_network_transmit_errs_total" -> Float.round(n * 50.0, 0)
      "node_load1" -> Float.round(0.5 + n * 4.0 + dh * 2.0, 2)
      "node_load5" -> Float.round(0.4 + n * 3.0 + dh * 1.5, 2)
      "node_load15" -> Float.round(0.3 + n * 2.0 + dh * 1.0, 2)
      "node_time_seconds" -> Float.round(ts_ms / 1_000.0, 3)
      "node_boot_time_seconds" -> Float.round(ts_ms / 1_000 - 86_400.0 * (1 + dh * 30), 0)
      "node_context_switches_total" -> Float.round(1.0e6 + ts_ms / 100 + n * 1.0e4, 0)
      "node_entropy_avail_bits" -> Float.round(2000.0 + n * 2000.0, 0)
      "node_filefd_allocated" -> Float.round(500.0 + n * 2000.0 + dh * 500.0, 0)
      "node_forks_total" -> Float.round(1.0e5 + ts_ms / 1.0e4 + n * 1000.0, 0)
      "node_intr_total" -> Float.round(5.0e7 + ts_ms / 100 + n * 1.0e5, 0)
      "node_procs_running" -> Float.round(1.0 + n * 8.0, 0)
      "node_procs_blocked" -> Float.round(n * 2.0, 0)
      "node_scrape_collector_duration_seconds" -> Float.round(0.001 + n * 0.05, 4)
      "node_scrape_collector_success" -> 1.0
      "node_nf_conntrack_entries" -> Float.round(100.0 + n * 5000.0 + dh * 1000.0, 0)
      "node_vmstat_pgfault" -> Float.round(1.0e7 + ts_ms / 1_000 + n * 5.0e4, 0)
      _ -> Float.round(50.0 + n * 50.0, 2)
    end
  end

  @doc """
  Generate a Prometheus text exposition body for one device scrape.

  Returns a string with one line per metric, ready to POST to
  `/api/v1/import/prometheus`.
  """
  def prometheus_body(dev_id, ts_ms) do
    host = "device_#{String.pad_leading(Integer.to_string(dev_id), 6, "0")}"

    @metrics
    |> Enum.map_join("\n", fn metric ->
      val = value(metric, ts_ms, dev_id)
      "#{metric}{host=\"#{host}\",env=\"prod\"} #{val} #{ts_ms}"
    end)
  end

  @doc """
  Generate a list of `{metric_name, labels, value}` tuples for one device.

  Ready to pass to `TimelessMetrics.write_batch/2`.
  """
  def write_batch_entries(dev_id, ts) do
    host = "device_#{String.pad_leading(Integer.to_string(dev_id), 6, "0")}"
    labels = %{"host" => host, "env" => "prod"}
    ts_ms = ts * 1000

    Enum.map(@metrics, fn metric ->
      {metric, labels, value(metric, ts_ms, dev_id), ts}
    end)
  end

  @doc """
  Generate `count` time series points for a single metric + device.

  Returns `[{timestamp, value}, ...]` sorted by timestamp.
  Useful for compression benchmarks.
  """
  def points(metric, dev_id, count, start_ts \\ 1_700_000_000, interval \\ 15) do
    for i <- 0..(count - 1) do
      ts = start_ts + i * interval
      ts_ms = ts * 1000
      {ts, value(metric, ts_ms, dev_id)}
    end
  end
end
