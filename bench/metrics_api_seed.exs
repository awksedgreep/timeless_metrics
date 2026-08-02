defmodule MetricsAPISeed do
  @moduledoc """
  Creates a fixed-cardinality, fixed-point-count Prometheus fixture for
  socket-level mechanical read comparisons. `--expected-existing-points`
  keeps the exact final-point assertion available for page-reuse runs against
  a pre-populated database.
  """

  @metrics [
    "node_cpu_seconds_total",
    "node_memory_MemAvailable_bytes",
    "node_memory_MemTotal_bytes",
    "node_memory_Buffers_bytes",
    "node_memory_Cached_bytes",
    "node_filesystem_avail_bytes",
    "node_filesystem_size_bytes",
    "node_disk_read_bytes_total",
    "node_disk_written_bytes_total",
    "node_disk_io_time_seconds_total",
    "node_network_receive_bytes_total",
    "node_network_transmit_bytes_total",
    "node_network_receive_errs_total",
    "node_network_transmit_errs_total",
    "node_load1",
    "node_load5",
    "node_load15",
    "node_time_seconds",
    "node_boot_time_seconds",
    "node_context_switches_total"
  ]

  def run do
    {:ok, _} = Application.ensure_all_started(:req)

    argv =
      case System.argv() do
        ["--" | rest] -> rest
        rest -> rest
      end

    {opts, _, _} =
      OptionParser.parse(argv,
        strict: [
          url: :string,
          devices: :integer,
          metrics: :integer,
          samples: :integer,
          first_timestamp: :integer,
          expected_existing_points: :integer
        ]
      )

    url = opts[:url] || "http://127.0.0.1:8428"
    devices = max(opts[:devices] || 200, 1)
    metrics = Enum.take(@metrics, max(opts[:metrics] || 20, 1))
    samples = max(opts[:samples] || 100, 1)
    expected_existing_points = max(opts[:expected_existing_points] || 0, 0)
    client = Req.new(base_url: url, retry: false, receive_timeout: 60_000)
    first_ms = (opts[:first_timestamp] || System.os_time(:second) - samples) * 1_000

    labels =
      for device <- 0..(devices - 1), metric <- metrics do
        host = "device_#{String.pad_leading(Integer.to_string(device), 6, "0")}"
        region = Enum.at(["us-east", "us-west", "eu-west", "ap-south"], rem(device, 4))
        env = Enum.at(["prod", "staging"], rem(device, 2))
        {metric, device, ~s(#{metric}{host="#{host}",region="#{region}",env="#{env}"})}
      end

    expected_series = devices * length(metrics)
    expected_points = expected_series * samples
    expected_total_points = expected_existing_points + expected_points
    started = System.monotonic_time(:microsecond)

    for sample <- 0..(samples - 1) do
      timestamp = first_ms + sample * 1_000

      body =
        labels
        |> Enum.map(fn {_metric, device, labeled} ->
          [
            labeled,
            " ",
            Float.to_string(device + sample / 100.0),
            " ",
            Integer.to_string(timestamp),
            "\n"
          ]
        end)
        |> IO.iodata_to_binary()

      case Req.post(client,
             url: "/api/v1/import/prometheus",
             body: body,
             headers: [{"content-type", "text/plain"}]
           ) do
        {:ok, %{status: 204}} -> :ok
        other -> raise "fixture write failed: #{inspect(other)}"
      end
    end

    case Req.post(client, url: "/api/v1/flush", body: "") do
      {:ok, %{status: 200, body: %{"completed_points" => ^expected_points}}} -> :ok
      other -> raise "fixture flush failed: #{inspect(other)}"
    end

    case Req.get(client, url: "/health") do
      {:ok,
       %{
         status: 200,
         body: %{"series" => ^expected_series, "points" => ^expected_total_points}
       }} ->
        :ok

      other ->
        raise "fixture health verification failed: #{inspect(other)}"
    end

    elapsed = System.monotonic_time(:microsecond) - started
    IO.puts("seeded_series=#{expected_series}")
    IO.puts("seeded_points=#{expected_points}")
    IO.puts("total_points=#{expected_total_points}")
    IO.puts("elapsed_us=#{elapsed}")
  end
end

MetricsAPISeed.run()
