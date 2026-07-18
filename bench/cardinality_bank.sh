#!/bin/bash
# Cardinality bank: fresh ephemeral container + empty data dir per scale,
# realistic_workload.exs --batch 50 --step-seconds 15 at 100K/250K/500K/1M
# series. Cross-machine methodology reference:
#   bench/results/2026-07-18_cardinality_bank_i185_v0.6.5.md
#
# Before running on a new machine:
#   - mise trust && mise install   (client MUST run the repo-pinned OTP —
#     an untrusted mise.toml silently falls back to the global toolchain
#     and costs ~15% client throughput; see the 14700HX results file)
#   - stop anything else bound to port 8428
#
# Usage:
#   bench/cardinality_bank.sh [outdir]
# Env overrides:
#   IMAGE   (default ghcr.io/awksedgreep/timeless-stack:latest)
#   NETWORK "published" (default, -p 8428:8428) or "host" — pasta port
#           forwarding costs ~13-18% at saturation; use "host" for engine
#           ceilings, "published" to match the i185 bank config
#   SCALES  device counts, default "5000 12500 25000 50000" (series = 20x)
#   DATA_MODE "hostdir" (default, bind-mount a /tmp dir — tmpfs on the Linux
#           bank machines) or "tmpfs" (tmpfs inside the container — use on
#           macOS podman machine, where a host bind mount goes through
#           virtiofs and bottlenecks flush I/O)
set -u
IMAGE=${IMAGE:-ghcr.io/awksedgreep/timeless-stack:latest}
NETWORK=${NETWORK:-published}
DATA_MODE=${DATA_MODE:-hostdir}
SCALES=${SCALES:-"5000 12500 25000 50000"}
OUT=${1:-bench_bank_$(date +%Y%m%d_%H%M)}
REPO=$(cd "$(dirname "$0")/.." && pwd)
SECRET=$(openssl rand -base64 48 | tr -d '\n')
mkdir -p "$OUT"

if [ "$NETWORK" = "host" ]; then NETFLAGS="--network=host"; else NETFLAGS="-p 8428:8428"; fi

for DEVICES in $SCALES; do
  LOG="$OUT/bank_${DEVICES}dev.log"
  if [ "$DATA_MODE" = "tmpfs" ]; then
    DATA=""
    DATAFLAGS="--mount type=tmpfs,destination=/data"
  else
    DATA=$(mktemp -d /tmp/tms_bench_data.XXXX)
    DATAFLAGS="-v $DATA:/data"
  fi
  echo "=== SCALE $DEVICES devices ($((DEVICES * 20)) series) ==="
  podman run -d --rm --name tms-bench $NETFLAGS \
    $DATAFLAGS \
    -e SECRET_KEY_BASE="$SECRET" \
    -e PHX_HOST=localhost \
    "$IMAGE" >/dev/null || { echo "container start failed"; exit 1; }

  for i in $(seq 1 60); do
    curl -sf -m 2 http://127.0.0.1:8428/health >/dev/null && break
    sleep 1
  done
  curl -sf -m 2 http://127.0.0.1:8428/health >/dev/null || { echo "health check failed"; podman rm -f tms-bench; exit 1; }
  echo "container healthy, starting workload $(date +%T)"

  (cd "$REPO" && TIMELESS_PORT=18428 TIMELESS_DATA_DIR=/tmp/tm_client_scratch \
    mise exec -- mix run bench/realistic_workload.exs \
      --tm-url http://127.0.0.1:8428 --vm-url "" \
      --devices "$DEVICES" --metrics 20 --batch 50 --step-seconds 15) \
    >"$LOG" 2>&1
  echo "workload done $(date +%T), exit=$?"

  echo "--- RSS after ---" >>"$LOG"
  podman stats --no-stream --format '{{.Name}} {{.MemUsage}}' tms-bench >>"$LOG" 2>&1
  grep -E 'TM peak|Saturated' "$LOG"

  podman rm -f -t 2 tms-bench >/dev/null 2>&1
  [ -n "$DATA" ] && rm -rf "$DATA"
  rm -rf /tmp/tm_client_scratch
  sleep 3
done
echo "=== BANK COMPLETE — logs in $OUT ==="
