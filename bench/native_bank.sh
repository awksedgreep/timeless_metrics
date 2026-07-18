#!/bin/bash
# Native (no-container) cardinality bank for macOS, where podman's gvproxy
# port forwarding caps HTTP ingest at ~1M pts/s and invalidates container
# benching. Server = `mix run --no-halt` (dev env), fresh data dir per scale,
# client = separate BEAM, same workload flags as the container banks.
# Usage: native_bank.sh <outdir>
# Env: SCALES (default "5000 12500 25000 50000"), DEFER (default false)
set -u
SCALES=${SCALES:-"5000 12500 25000 50000"}
DEFER=${DEFER:-false}
OUT=${1:?outdir required}
REPO=/Users/mcotner/Documents/elixir/timeless/timeless_metrics
mkdir -p "$OUT"

for DEVICES in $SCALES; do
  LOG="$OUT/bank_${DEVICES}dev.log"
  DATA=$(mktemp -d /tmp/tms_native_data.XXXX)
  echo "=== SCALE $DEVICES devices ($((DEVICES * 20)) series) defer=$DEFER ==="
  (cd "$REPO" && TIMELESS_PORT=8430 TIMELESS_DATA_DIR="$DATA" \
    TIMELESS_DEFER_COMPRESSION="$DEFER" \
    mise exec -- mix run --no-halt) >"$OUT/server_${DEVICES}dev.log" 2>&1 &
  SH_PID=$!

  HEALTHY=0
  for i in $(seq 1 60); do
    curl -sf -m 2 http://127.0.0.1:8430/health >/dev/null && { HEALTHY=1; break; }
    sleep 1
  done
  [ "$HEALTHY" = 1 ] || { echo "server failed to start"; exit 1; }
  # client BEAM not started yet, so the newest beam.smp is the server
  SERVER_BEAM=$(pgrep -n beam.smp)
  echo "server healthy (beam pid $SERVER_BEAM), starting workload $(date +%T)"

  (cd "$REPO" && TIMELESS_PORT=18428 TIMELESS_DATA_DIR=/tmp/tm_client_scratch \
    mise exec -- mix run bench/realistic_workload.exs \
      --tm-url http://127.0.0.1:8430 --vm-url "" \
      --devices "$DEVICES" --metrics 20 --batch 50 --step-seconds 15) \
    >"$LOG" 2>&1
  echo "workload done $(date +%T), exit=$?"

  echo "--- RSS after (KB) ---" >>"$LOG"
  ps -o rss= -p "$SERVER_BEAM" >>"$LOG" 2>&1
  echo "--- data dir size ---" >>"$LOG"
  du -sh "$DATA" >>"$LOG" 2>&1
  grep -E 'TM peak|Saturated' "$LOG"

  kill "$SERVER_BEAM" 2>/dev/null
  kill "$SH_PID" 2>/dev/null
  wait "$SH_PID" 2>/dev/null
  rm -rf "$DATA" /tmp/tm_client_scratch
  sleep 3
done
echo "=== NATIVE BANK COMPLETE — logs in $OUT ==="
