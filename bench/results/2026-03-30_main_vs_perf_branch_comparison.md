# Main vs Perf Branch Comparison

Date: 2026-03-30
Compared branches:

- `main`
- `perf/rust-engine-optimizations`

Comparison method:

- Sequential benchmark runs only
- No overlapping benchmark processes
- Equivalent workloads run on both branches

## Series Path

Workload:

- `20,000` unseen series
- Focus: series resolution and first write for new series

### Main

- Memory-only:
  - Resolve new series: `1,277/sec`
  - Resolve cached series: `1,443,313/sec`
  - First write for new series: `1,274/sec`
  - Flush after first write: `103ms`
- Disk:
  - Resolve new series: `1,271/sec`
  - Resolve cached series: `1,359,064/sec`
  - First write for new series: `1,299/sec`
  - Flush after first write: `103ms`

### Perf Branch

- Memory-only:
  - Resolve new series: `486,558/sec`
  - Resolve cached series: `3,808,073/sec`
  - First write for new series: `410,686/sec`
  - Flush after first write: `104ms`
- Disk:
  - Resolve new series: `501,102/sec`
  - Resolve cached series: `3,570,790/sec`
  - First write for new series: `414,585/sec`
  - Flush after first write: `103ms`

### Takeaway

- New-series resolution improved by roughly `380x-395x`
- First-write throughput for unseen series improved by roughly `315x-325x`
- Flush cost stayed effectively unchanged

## High-Fanout Query Path

Workload:

- `12,000` matching series
- `60` points per series
- Focus: multi-series range and aggregate query latency

### Main

- Memory-only:
  - Range query median: `210.02ms`
  - Aggregate query median: `210.11ms`
- Disk:
  - Range query median: `202.37ms`
  - Aggregate query median: `210.66ms`

### Perf Branch

- Memory-only:
  - Range query median: `204.89ms`
  - Aggregate query median: `14.54ms`
- Disk:
  - Range query median: `205.96ms`
  - Aggregate query median: `15.54ms`

### Takeaway

- Range-query latency is roughly flat versus `main`
- Unbucketed multi-series aggregate latency improved by roughly `13x-14x`

## Summary

The branch materially improves two important areas:

- unseen-series ingest and first-write throughput
- unbucketed multi-series aggregate latency

The branch does not materially improve high-fanout raw range-query latency, which remains around `~200ms` for `12,000` series and appears acceptable for now.
