# Cardinality Bank — Apple M5 Pro, Native Server, Defer-On (0.6.6 engine)

**Date:** 2026-07-18
**Machine:** Apple M5 Pro, 18 cores, 64GB unified memory, macOS 26.5.
**Server:** timeless_metrics 6.1.2 (repo HEAD — the exact engine in the
timeless-stack:0.6.6 image), run **natively** via `mix run --no-halt`
(dev env), TIMELESS_DEFER_COMPRESSION=true at every scale (the 0.6.6
container default posture). Data dir on APFS SSD (/tmp), fresh per scale.
**Client:** co-located separate BEAM, repo-pinned OTP 29.0.2 /
Elixir 1.20.2 via mise, same host.
**Workload:** `realistic_workload.exs --batch 50 --step-seconds 15`,
scales 100K/250K/500K/1M series — identical flags to the Intel banks.
**Script:** `bench/native_bank.sh` (DEFER=true).

## Why native, not the container (important)

The container bank is **invalid on macOS**: podman runs containers in an
applehv VM and forwards published ports through gvproxy (user-mode
proxy). Behind published ports, both the 0.6.5 and 0.6.6 images walled
at 924–965K pts/s with **server CPU under 0.7 of 12 vCPUs** — the proxy
is the bottleneck, and defer on/off makes no difference there.
`--network=host` is not reachable from the macOS host, so there is no
container path around it. The same 100K workload against a native server
did 14.0M pts/s. Operationally: a stack container deployed on macOS via
published ports caps around ~1M pts/s regardless of hardware; on Linux
the equivalent tax (pasta) is only ~13–18%.

## Bank results vs the Intel machines

| Series | M5 Pro native | i185 0.6.5 bank | i185 defer-on | 14700HX 0.6.6 |
|-------:|--------------:|----------------:|--------------:|--------------:|
| 100K   | **15.8M**     | 5.4M            | —             | 3.4M          |
| 250K   | **14.3M**     | 6.0M            | —             | 3.9M          |
| 500K   | **15.3M**     | 6.8M            | —             | 3.9M          |
| 1M     | **13.7M**     | 3.9M            | 6.7M          | 3.5M          |

- All scales: 0 query errors; series counts verified exact.
- **Every scale saturated on client ramp shortfall (40–47% of target),
  never on server latency.** The engine never tripped the 100ms write
  p99 ceiling at any scale — unprecedented across the three machines.
  The recorded peaks are a floor on this machine's engine capacity; the
  co-located client is the limiter.
- Peak is flat 13.7–15.8M pts/s across 100K→1M series: with defer on,
  **no cardinality penalty at 1M** (matches the i185 defer-on finding,
  at 2.0x the throughput).
- vs i185: 2.0–2.9x. vs 14700HX: 3.7–4.6x. The 14700HX writeup's
  prediction — unified-memory bandwidth beats core count on this
  workload — is confirmed emphatically.

## Clean zone (write p99 < 2ms) — 4–5x the Intel banks

| Series | Clean zone       | p99 at that step | First degraded step   |
|-------:|-----------------:|-----------------:|-----------------------|
| 100K   | **11.2M pts/s**  | 1.37ms           | 15.8M @ 3.53ms        |
| 250K   | 7.6M pts/s       | 1.03ms           | 13.4M @ 6.72ms        |
| 500K   | 7.8M pts/s       | 1.75ms           | 13.0M @ 16.1ms        |
| 1M     | 7.8M pts/s       | 1.69ms           | 12.3M @ 42.1ms        |

Both Intel machines held ~2M pts/s sub-2ms; this machine holds
**7.8–11.2M pts/s sub-2ms through 1M series**. Sub-millisecond p99
(620–700us) holds through ~6–8M pts/s at every scale.

## Query latency under write load

Query p99 stays 1.4–2.7ms inside the clean zone at every scale
(20 workers, ramping with writes) and degrades with write latency past
it, worst 61.5ms p99 at the 1M-series 13.7M pts/s peak. 0 errors —
343K queries across the bank.

## Memory / disk notes

- RSS after: 14.5–17.2GiB at all scales — the usual write-buffer
  high-water at ramp stop, here inflated by ~2x-Intel ingest volume
  (476–565M points per run). Baseline cardinality footprint is not
  readable from this number (see the 1M probe file). 64GB absorbed it
  without pressure; bound with memory_budget_mb where needed.
- Data dir at run end was only 9–84MB: at these ingest rates nearly the
  entire run volume was still in raw buffers when the ramp ended, i.e.
  flush drain lags a 13M+ pts/s burst. Consistent with the RSS
  interpretation; verification still returned exact series/point counts.

## Caveats

- Native server vs the Intel banks' containers: no container/netns tax
  on this run (Linux banks paid pasta ~13–18% at saturation). Engine
  version is identical to the 0.6.6 image; HTTP path (bandit) is the
  same code.
- Client and server share the 18 cores, as on both Intel banks. A
  remote-client rerun is the obvious follow-up to find the true server
  ceiling, since every scale here ended client-limited.
