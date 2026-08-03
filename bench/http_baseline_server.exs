defmodule HTTPBaselineServer do
  @moduledoc """
  Starts one isolated TimelessMetrics HTTP control process for POC baselines.

      mix run bench/http_baseline_server.exs -- \
        --engine libsql --port 19442 --data-dir /tmp/tm-libsql-control \
        --readers 2 --ingest-workers 4 --maintenance deferred

  The process prints its OS PID so Linux `VmHWM` can be sampled from `/proc`.
  `--maintenance deferred` retains normal write buffering and explicit flushes,
  but moves scheduled rollup/retention work outside a benchmark run.
  """

  def run do
    argv =
      case System.argv() do
        ["--" | rest] -> rest
        rest -> rest
      end

    {opts, _, invalid} =
      OptionParser.parse(argv,
        strict: [
          engine: :string,
          port: :integer,
          data_dir: :string,
          readers: :integer,
          ingest_workers: :integer,
          maintenance: :string
        ]
      )

    if invalid != [], do: raise("invalid options: #{inspect(invalid)}")

    engine = parse_engine(opts[:engine] || "libsql")
    port = opts[:port] || 19_442

    data_dir =
      opts[:data_dir] ||
        Path.join(
          System.tmp_dir!(),
          "timeless_metrics_http_#{engine}_#{System.os_time(:millisecond)}"
        )

    readers = max(opts[:readers] || 2, 1)
    ingest_workers = max(opts[:ingest_workers] || 4, 1)
    maintenance = parse_maintenance(opts[:maintenance] || "deferred")
    schema = schema_for(maintenance)

    # `mix run --no-start` avoids booting the developer-configured default
    # store. Start the application only after clearing that opt-in setting so
    # this process owns exactly the isolated control store below.
    Application.delete_env(:timeless_metrics, :data_dir)
    {:ok, _started_apps} = Application.ensure_all_started(:timeless_metrics)

    File.mkdir_p!(data_dir)

    children = [
      {TimelessMetrics,
       name: :http_baseline,
       engine: engine,
       data_dir: data_dir,
       schema: schema,
       reader_pool_size: readers,
       ingest_workers: ingest_workers,
       scraping: false,
       self_monitor: false},
      {TimelessMetrics.HTTP, store: :http_baseline, port: port}
    ]

    {:ok, _supervisor} =
      Supervisor.start_link(children,
        strategy: :one_for_one,
        name: HTTPBaselineServer.Supervisor
      )

    IO.puts("HTTP_BASELINE_READY")
    IO.puts("pid=#{System.pid()}")
    IO.puts("engine=#{engine}")
    IO.puts("port=#{port}")
    IO.puts("data_dir=#{data_dir}")
    IO.puts("readers=#{readers}")
    IO.puts("ingest_workers=#{ingest_workers}")
    IO.puts("maintenance=#{maintenance}")

    Process.sleep(:infinity)
  end

  defp parse_engine("libsql"), do: :libsql
  defp parse_engine("rust"), do: :rust
  defp parse_engine(other), do: raise("--engine must be libsql or rust, got: #{inspect(other)}")

  defp parse_maintenance("deferred"), do: :deferred
  defp parse_maintenance("default"), do: :default

  defp parse_maintenance(other),
    do: raise("--maintenance must be deferred or default, got: #{inspect(other)}")

  defp schema_for(:default), do: TimelessMetrics.Schema.default()

  defp schema_for(:deferred) do
    %{
      TimelessMetrics.Schema.default()
      | rollup_interval: :timer.hours(24),
        retention_interval: :timer.hours(24)
    }
  end
end

HTTPBaselineServer.run()
