defmodule MetricsAPIPromQL do
  @moduledoc """
  Socket-to-body Session 4 benchmark and optional black-box differential.

  Both targets must be seeded with `metrics_api_seed.exs` using the same
  `--first-timestamp`. The benchmark compares decoded Prometheus envelopes
  before timing either process, then reports the selected target's latency.
  """

  def run do
    {:ok, _} = Application.ensure_all_started(:req)

    argv =
      case System.argv() do
        ["--" | rest] -> rest
        rest -> rest
      end

    {opts, _, invalid} =
      OptionParser.parse(argv,
        strict: [url: :string, reference_url: :string, runs: :integer, wide_runs: :integer]
      )

    if invalid != [], do: raise("invalid options: #{inspect(invalid)}")
    url = opts[:url] || "http://127.0.0.1:19439"
    reference_url = opts[:reference_url]
    runs = max(opts[:runs] || 300, 1)
    wide_runs = max(opts[:wide_runs] || 50, 1)
    client = client(url)
    reference = reference_url && client(reference_url)
    fixture_stop = fixture_stop(client)

    if reference && fixture_stop(reference) != fixture_stop do
      raise("targets do not have the same fixture timestamp")
    end

    start = fixture_stop - 99
    exact = ~s(node_load1{host="device_000000"})
    multi = ~s(node_load1{env="prod"})

    shapes = [
      {"selector_exact_range", runs, "/prometheus/api/v1/query_range",
       [query: exact, start: start, end: fixture_stop, step: 1]},
      {"selector_multi_range", wide_runs, "/prometheus/api/v1/query_range",
       [query: multi, start: start, end: fixture_stop, step: 1]},
      {"avg_exact_range", runs, "/prometheus/api/v1/query_range",
       [query: "avg_over_time(#{exact}[60s])", start: start, end: fixture_stop, step: 1]},
      {"avg_multi_range", wide_runs, "/prometheus/api/v1/query_range",
       [query: "avg_over_time(#{multi}[60s])", start: start, end: fixture_stop, step: 1]},
      {"selector_exact_instant", runs, "/prometheus/api/v1/query",
       [query: exact, time: fixture_stop]},
      {"avg_exact_instant", runs, "/prometheus/api/v1/query",
       [query: "avg_over_time(#{exact}[60s])", time: fixture_stop]}
    ]

    if reference do
      Enum.each(shapes, fn {name, _count, path, params} ->
        actual = request!(client, path, params) |> :json.decode()
        expected = request!(reference, path, params) |> :json.decode()
        if actual != expected, do: raise("PromQL differential mismatch for #{name}")
      end)

      IO.puts("# differential=6/6 reference=#{reference_url}")
    end

    IO.puts("# url=#{url} fixture_start=#{start} fixture_stop=#{fixture_stop}")
    IO.puts("shape,median_us,p95_us,p99_us,min_us,max_us,runs,response_bytes")

    Enum.each(shapes, fn {name, count, path, params} ->
      for _ <- 1..10, do: request!(client, path, params)

      samples =
        for _ <- 1..count do
          {elapsed, body} = :timer.tc(fn -> request!(client, path, params) end)
          {elapsed, byte_size(body)}
        end

      latencies = samples |> Enum.map(&elem(&1, 0)) |> Enum.sort()

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
            samples |> hd() |> elem(1)
          ],
          ","
        )
      )
    end)
  end

  defp client(url), do: Req.new(base_url: url, retry: false, receive_timeout: 60_000)

  defp fixture_stop(client) do
    client
    |> request!("/api/v1/query", metric: "node_load1", host: "device_000000")
    |> :json.decode()
    |> Map.fetch!("timestamp")
  end

  defp request!(client, path, params) do
    case Req.get(client, url: path, params: params, decode_body: false) do
      {:ok, %{status: 200, body: body}} when is_binary(body) -> body
      other -> raise "PromQL request failed: #{inspect(other)}"
    end
  end

  defp percentile(values, percentile) do
    index = max(ceil(length(values) * percentile / 100) - 1, 0)
    Enum.at(values, index)
  end
end

MetricsAPIPromQL.run()
