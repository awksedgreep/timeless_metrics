# Cardinality Bank — Container 0.6.6 on i7-14700HX (co-located client)

**Date:** 2026-07-18
**Machine:** Intel Core i7-14700HX (8P+12E, 28 threads), 31GB RAM, Linux
(Arch, kernel 7.0.10). Data dir on tmpfs (/tmp).
**Image:** ghcr.io/awksedgreep/timeless-stack:0.6.6 (timeless_metrics 6.1.2),
built 2026-07-18 18:14 UTC. defer_compression ON (0.6.6 container default).
**Method:** identical to the i185 banks — fresh ephemeral container + empty
data dir per scale, `realistic_workload.exs --batch 50 --step-seconds 15`,
scales 100K/250K/500K/1M series. Client BEAM co-located on the same host,
container ports published (rootless podman 5.8.2, pasta 2026_05_26).

## Bank results vs i185

| Series | 14700HX peak | i185 peak (0.6.5) | Saturation mode (14700HX)   | RSS after |
|-------:|-------------:|------------------:|-----------------------------|----------:|
| 100K   | 3.4M         | 5.4M              | ramp shortfall (51%)        | 4.56GB    |
| 250K   | 3.9M         | 6.0M              | ramp shortfall (48%)        | 6.52GB    |
| 500K   | 3.9M         | 6.8M              | server write p99 118ms      | 7.17GB    |
| 1M     | 3.5M         | 3.9M (6.7M defer-on) | server write p99 174ms   | 5.55GB    |

- All scales: 0 query errors, series counts verified exact.
- Clean zone (write p99 < 2ms) is ~2M pts/s at every scale through 1M —
  matches i185. At 1M series: 2.0M pts/s @ 1.49ms p99.
- Peaks are 55–75% of the i185 bank despite the nominally stronger CPU.

## Diagnostics (500K scale A/Bs)

| Config                              | Peak     | Saturation           |
|-------------------------------------|---------:|----------------------|
| Bank config (pasta, batch 50, defer on)  | 3.9M | write p99 118ms      |
| defer_compression=false             | 3.9M     | write p99 111ms      |
| --network=host (no pasta)           | 4.6M     | ramp shortfall (56%) |
| --network=host + --batch 100        | **5.1M** | write p99 66–93ms    |

- **defer on/off: no difference** at 500K — the 0.6.6 default is not the
  regression; its win is specifically the 1M flush wall (untested here at
  1M defer-off vs on; the bank 1M ran defer-on and still hit a 174ms wall).
- **pasta port-forwarding costs ~18%** at saturation on this setup
  (3.9M → 4.6M with host networking). The published-port quadlet pays this
  in production too.
- **Client generation is the next wall**: halving request count
  (batch 100) moved 4.6M → 5.1M, at which point server write p99 (66ms+)
  is also saturating. Co-located ceiling ≈ 5M pts/s on this machine.
- Not thermal/power: package 67°C at full load, EPP=performance, on AC,
  no clock capping observed. Not memory/tmpfs pressure (watched; never
  tripped). Both BEAMs split the box ~13 cores each at saturation.

## Root cause (confirmed by follow-up A/Bs)

The i185 bank's client was also co-located, so the gap was real. Two
causes found:

**1. Client toolchain: OTP 28 vs the repo-pinned OTP 29.** The bank
above ran the client on the machine-global Elixir 1.19.5/OTP 28 because
the repo's mise.toml (erlang 29.0.2, elixir 1.20.2-otp-29 — what the
i185 used) was untrusted on this machine. Rerun at 500K after
`mise trust && mise install`:

| Config (500K, batch 50)     | OTP 28 | OTP 29 |
|-----------------------------|-------:|-------:|
| Bank config (pasta)         | 3.9M   | 4.6M (+18%) |
| --network=host              | 4.6M   | 5.2M (+13%) |

**2. Memory bandwidth (structural).** Measured STREAM triad:
58.3 GB/s (dual-channel DDR5-5600 SODIMM, Samsung M425R). A 185H with
LPDDR5X-7467 measures ~85-100 GB/s. This workload is data movement —
parse/term-build/buffer/flush, doubled by co-locating the generator —
and all 28 threads share that 58 GB/s. This is the residual: at
apples/apples config (OTP 29, bank config) this machine does 4.6M vs
the i185's 6.8M at 500K; best co-located result here is 5.2M.

Conclusion: on this co-located workload, memory bandwidth and latency
matter more than core count or clocks. Not thermal/power (PL1 115W,
67°C, no capping). Expectation: Apple Silicon (unified memory,
150+ GB/s, lower latency) should clear both Intel machines; rerun the
bank there per the 0.6.5 reference file, changing only the Machine line.
