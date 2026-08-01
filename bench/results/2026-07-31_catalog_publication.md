# libSQL catalog publication through TimelessMetrics — 2026-07-31

This is the TimelessMetrics-side Session 6 result paired with the direct
extension catalog-publication benchmark. No adapter-specific shortcut was
added: the public API benefits from the same loadable-extension transaction
behavior available to direct SQLite and libSQL users.

## Reproduction

Starting TimelessMetrics revision:
`d355d3d1ed43ad2f77e90e47b7a3455d57ba9d6b` on
`feat/libsql-storage-engine`, with the migration work uncommitted. The paired
extension revision was `a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69` on
`feat/timeless-metrics-embedding`, also dirty.

The local paired NIFs were forced to compile from source before running:

```sh
elixir -e 'Application.put_env(:elixir_make, :force_build, timeless_metrics: true); \
  Mix.start(); Mix.env(:test); \
  Mix.Project.in_project(:timeless_metrics, ".", fn _ -> \
    Mix.Task.run("compile", []); Mix.Task.reenable("run"); \
    Mix.Task.run("run", ["--no-compile", "bench/engine_query_bench.exs", \
      "--engine", "libsql", "--runs", "10"]) \
  end)'
```

Environment: Linux 7.1.3 x86-64, Intel Core Ultra 9 185H, 22 BEAM
schedulers, Elixir 1.20.2, OTP 29, bundled SQLite 3.53.2, and a `/tmp`
database on `tmpfs`. The deterministic workload is 12,000 series with 60
points each, or 720,000 total points.

## Result

| Measurement | Session 0 baseline | Session 6 | Change |
|---|---:|---:|---:|
| First exact public read after flush | 54.818ms | 2.891ms | 18.96x faster |
| Session 6 release gate | 5ms or less | 2.891ms | pass |
| No-pending writer barrier, median / p95 | 9 / 17us | 8 / 14us | unchanged |

The first read includes the Elixir public API, writer barrier, Exqlite
statement execution, extension query, packed decoding, and result shaping. The
direct extension itself measured a 0.187ms fresh-process median, so the
remaining roughly 2.7ms is public-boundary and one-off runtime work rather than
an authoritative catalog reload.

Representative warm figures from this process were 0.407ms exact raw,
1.881ms narrow raw, 112.049ms full raw, 40.176ms scalar aggregate, and
127.508ms bucketed average. They remain subject to the existing `powersave`
governor variance and are not used as new release comparisons.

The correctness contract is pinned below this adapter: commit publishes the
prepared token, rollback discards it, compaction and prune preserve an already
open reader's exact view, and a separate process still invalidates the local
fast path. The direct design, samples, and tests are in the paired
`timeless-libsql/tools/bench/results/2026-07-31_catalog_publication.md` record.
