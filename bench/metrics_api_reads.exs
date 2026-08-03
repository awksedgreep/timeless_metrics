defmodule MetricsAPIReads do
  @moduledoc """
  Sequential socket-to-body benchmark for the Session 3 mechanical metrics
  routes. Run it against identically seeded Elixir+libSQL and Rust API
  processes; this script does not create or mutate fixture data.
  """

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
          runs: :integer,
          discovery_runs: :integer,
          lookback_seconds: :integer
        ]
      )

    url = opts[:url] || "http://127.0.0.1:8428"
    runs = max(opts[:runs] || 500, 1)
    discovery_runs = max(opts[:discovery_runs] || 50, 1)
    lookback = max(opts[:lookback_seconds] || 3_600, 15)
    if rem(lookback, 15) != 0, do: raise("--lookback-seconds must be divisible by 15")
    client = Req.new(base_url: url, retry: false, receive_timeout: 60_000)

    fixture_stop =
      client
      |> request!("/api/v1/query", metric: "node_load1", host: "device_000000")
      |> :json.decode()
      |> Map.fetch!("timestamp")

    # Inclusive bounds contain exactly `lookback` seconds, keeping the common
    # 15-second range on the extension's packed native-window path. Anchoring
    # to the fixture's latest sample also gives separately seeded controls the
    # same bucket phase and response cardinality.
    range_start = fixture_stop - lookback + 1
    export_start = fixture_stop - lookback

    shapes = [
      {"latest_exact", runs, "/api/v1/query", [metric: "node_load1", host: "device_000000"]},
      {"range_exact", runs, "/api/v1/query_range",
       [
         metric: "node_load1",
         host: "device_000000",
         from: range_start,
         to: fixture_stop,
         step: 15,
         aggregate: "avg"
       ]},
      {"export_exact", runs, "/api/v1/export",
       [
         metric: "node_load1",
         host: "device_000000",
         from: export_start,
         to: fixture_stop
       ]},
      {"label_names", discovery_runs, "/api/v1/labels", []},
      {"label_values_metric", discovery_runs, "/api/v1/label/host/values",
       [metric: "node_load1"]},
      {"series_metric", discovery_runs, "/api/v1/series", [metric: "node_load1"]},
      {"series_selector", discovery_runs, "/prometheus/api/v1/series",
       [{"match[]", ~s(node_load1{env="prod"})}]}
    ]

    IO.puts("# url=#{url}")

    IO.puts(
      "# runs=#{runs} discovery_runs=#{discovery_runs} " <>
        "lookback_seconds=#{lookback} fixture_stop=#{fixture_stop}"
    )

    IO.puts("shape,median_us,p95_us,p99_us,min_us,max_us,runs,response_bytes")

    Enum.each(shapes, fn {name, count, path, params} ->
      for _ <- 1..10, do: request!(client, path, params)

      samples =
        for _ <- 1..count do
          {elapsed, body} = :timer.tc(fn -> request!(client, path, params) end)
          {elapsed, byte_size(body)}
        end

      latencies = samples |> Enum.map(&elem(&1, 0)) |> Enum.sort()
      response_bytes = samples |> hd() |> elem(1)

      IO.puts(
        Enum.join(
          [
            name,
            percentile(latencies, 50),
            percentile(latencies, 95),
            percentile(latencies, 99),
            hd(latencies),
            List.last(latencies),
            count,
            response_bytes
          ],
          ","
        )
      )
    end)
  end

  defp request!(client, path, params) do
    case Req.get(client, url: path, params: params, decode_body: false) do
      {:ok, %{status: 200, body: body}} when is_binary(body) -> body
      other -> raise "mechanical read failed: #{inspect(other)}"
    end
  end

  defp percentile(values, percentile) do
    index = max(ceil(length(values) * percentile / 100) - 1, 0)
    Enum.at(values, index)
  end
end

MetricsAPIReads.run()
