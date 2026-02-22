# TSBS Benchmark Runner for TimelessMetrics
#
# Usage: mix run bench/tsbs_bench.exs
#
# Starts TimelessMetrics HTTP on port 8429, then prints commands to run TSBS.
# After loading data and running queries, Ctrl+C to exit.

defmodule TSBSBench do
  @port 8429
  @data_dir "/tmp/timeless_tsbs_bench_#{System.os_time(:millisecond)}"

  def run do
    IO.puts("\n=== TimelessMetrics TSBS Benchmark ===\n")
    IO.puts("Data dir: #{@data_dir}")
    IO.puts("HTTP port: #{@port}\n")

    # Start TimelessMetrics with tuned settings
    shards = System.schedulers_online()

    {:ok, _sup} =
      Supervisor.start_link(
        [
          {TimelessMetrics,
           name: :tsbs_bench, data_dir: @data_dir, buffer_shards: shards},
          {TimelessMetrics.HTTP, store: :tsbs_bench, port: @port}
        ],
        strategy: :one_for_one
      )

    IO.puts("TimelessMetrics HTTP listening on http://localhost:#{@port}")
    IO.puts("Health check: http://localhost:#{@port}/health\n")

    IO.puts("=== Run these commands in another terminal ===\n")

    IO.puts("""
    # 1. Generate data (small scale for initial test)
    tsbs_generate_data \\
      --use-case=devops --seed=123 --scale=100 \\
      --timestamp-start="2024-01-01T00:00:00Z" \\
      --timestamp-end="2024-01-01T01:00:00Z" \\
      --log-interval="10s" --format="influx" \\
      > /tmp/tsbs_data.txt

    # 2. Load data
    tsbs_load_victoriametrics \\
      --urls="http://localhost:#{@port}/write" \\
      --file=/tmp/tsbs_data.txt --workers=4

    # 3. Generate queries
    tsbs_generate_queries \\
      --use-case=devops --seed=123 --scale=100 \\
      --timestamp-start="2024-01-01T00:00:00Z" \\
      --timestamp-end="2024-01-01T01:00:00Z" \\
      --queries=100 --query-type=single-groupby-1-1-1 \\
      --format=victoriametrics \\
      > /tmp/tsbs_queries.txt

    # 4. Run queries
    tsbs_run_queries_victoriametrics \\
      --urls="http://localhost:#{@port}" \\
      --file=/tmp/tsbs_queries.txt --workers=4

    # --- Other query types ---
    # single-groupby-1-1-1, single-groupby-1-8-1, single-groupby-5-1-1
    # single-groupby-5-8-1, double-groupby-1, double-groupby-5
    # double-groupby-all, cpu-max-all-1, cpu-max-all-8
    """)

    IO.puts("Press Ctrl+C to stop the server and clean up.\n")

    # Keep alive
    Process.sleep(:infinity)
  end
end

TSBSBench.run()
