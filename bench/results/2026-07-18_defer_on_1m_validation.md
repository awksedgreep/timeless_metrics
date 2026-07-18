# Raw-First Compaction at 1M Series — Defer-On Validation (i185)

**Date:** 2026-07-18
**Machine:** Intel Core Ultra 9 185H ("i185"), Linux, 22 schedulers
**Image:** ghcr.io/awksedgreep/timeless-stack:0.6.5 with
TIMELESS_DEFER_COMPRESSION=true (fresh ephemeral container, empty data dir)
**Workload:** `realistic_workload.exs --devices 50000 --metrics 20 --batch 50`
(1.0M series), identical to the defer-off banks.

## Result: the 1M-series ingest wall moves from 3.9M to 6.7M pts/s (+72%)

| Interval | Pts/s  | Write p50 | Write p99 |
|----------|-------:|----------:|----------:|
| 1.0s     | 999.2K | 727us     | 1.35ms    |
| 500ms    | 2.0M   | 600us     | 1.21ms    |
| 250ms    | 3.9M   | 821us     | 68.5ms *  |
| 125ms    | 6.5M   | 19.5ms    | 72.1ms    |
| 62ms     | **6.7M** | 76.7ms  | 123.9ms   |

\* transient flush-storm patch that defer-off could not push through;
with raw flushes the engine stabilizes past it and keeps climbing.

- Every defer-off 1M run (0.6.4 and 0.6.5 images) saturated at 3.9M
  pts/s on server write p99. With raw-first flushes the same workload
  completes 6.5M and 6.7M steps — **peak ingest at 1M series now equals
  the 100K-500K peaks (6.4-6.8M)**: the cardinality penalty on peak
  throughput is eliminated.
- 327.9M points ingested in-run (vs ~150M in defer-off runs); 18.4K
  queries, 0 errors.
- RSS after: 10.6GB — proportional to the 2.3x larger volume buffered
  at 1.7x the rate; bound with memory_budget_mb where needed.

## Operational note

Container shutdown at this buffer volume exceeded podman's default 10s
SIGTERM window (SIGKILL followed; final buffered points lost in the
bench container). Production quadlets running high sustained ingest
should set a generous TimeoutStopSec so the shutdown flush completes.

## Outcome

timeless_stack 0.6.6 defaults TIMELESS_DEFER_COMPRESSION=true (container
posture: server-class disks, ingest-heavy). The embedded library default
remains false (unknown hardware, ~7x disk write amplification).
Opt out in containers with TIMELESS_DEFER_COMPRESSION=false.
