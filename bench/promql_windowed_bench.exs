# PromQL windowed-evaluator benchmark — the "measured, accepted cost" guard
# promised by notes/promql_vm_parity_plan_2026-07-24.md.
#
# Usage: mix run --no-start bench/promql_windowed_bench.exs
# Record results in bench/results/ when hardware or evaluator changes.

for app <- [:telemetry, :exqlite, :ezstd, :ex_alp, :ex_openzl, :rustler, :rocket] do
  Application.ensure_all_started(app)
end

{:ok, _} =
  Supervisor.start_link(
    [{TimelessMetrics, name: :bench, data_dir: "/tmp/timeless_promql_bench_#{System.os_time(:millisecond)}"}],
    strategy: :one_for_one
  )

alias TimelessMetrics.PromQL

# 200 series x 24h @ 15s = 1.152M points
base = 1_700_000_000
hosts = for i <- 1..200, do: "host_#{i}"
points_per_series = div(86_400, 15)

IO.puts("seeding 200 series x #{points_per_series} pts = #{200 * points_per_series} points...")

{seed_us, _} =
  :timer.tc(fn ->
    for host <- hosts do
      entries =
        for i <- 0..(points_per_series - 1) do
          {"cpu", %{"host" => host}, 50.0 + :erlang.phash2({host, i}, 100) / 10, base + i * 15}
        end

      TimelessMetrics.write_batch(:bench, entries)
    end

    TimelessMetrics.flush(:bench)
  end)

IO.puts("seeded in #{div(seed_us, 1000)}ms\n")

run = fn label, query, from, to, step ->
  {:ok, ast} = PromQL.parse(query)
  # warm
  {:ok, _} = PromQL.execute(ast, :bench, from, to, step)

  {us, {:ok, resp}} = :timer.tc(fn -> PromQL.execute(ast, :bench, from, to, step) end)
  n_series = length(resp["data"]["result"])

  n_points =
    Enum.reduce(resp["data"]["result"], 0, fn s, acc -> acc + length(s["values"]) end)

  IO.puts(
    "#{String.pad_trailing(label, 46)} #{String.pad_leading("#{div(us, 1000)}ms", 8)}  → #{n_series} series, #{n_points} pts"
  )
end

day_end = base + 86_400

IO.puts("--- dashboard-shaped (window ≈ step) ---")
run.("selector, 1h range, 60s step", "cpu", day_end - 3600, day_end, 60)
run.("selector, 24h range, 300s step", "cpu", base, day_end, 300)
run.("rate 5m, 24h range, 300s step", "rate(cpu[5m])", base, day_end, 300)
run.("sum by host (rate 5m), 24h, 300s", "sum by (host) (rate(cpu[5m]))", base, day_end, 300)
run.("avg(cpu), 24h range, 300s step", "avg(cpu)", base, day_end, 300)
run.("cpu / 10, 24h range, 300s step", "cpu / 10", base, day_end, 300)

IO.puts("\n--- the hot spot: window >> step ---")
run.("max_over_time 1h window, 6h range, 60s", "max_over_time(cpu[1h])", day_end - 21_600, day_end, 60)
run.("max_over_time 6h window, 6h range, 60s", "max_over_time(cpu[6h])", day_end - 21_600, day_end, 60)
run.("avg_over_time 1h window, 6h range, 60s", "avg_over_time(cpu[1h])", day_end - 21_600, day_end, 60)

IO.puts("\n--- subquery ---")
run.("max_over_time(rate[5m])[1h:1m], 6h, 300s", "max_over_time(rate(cpu[5m])[1h:1m])", day_end - 21_600, day_end, 300)
