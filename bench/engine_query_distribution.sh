#!/usr/bin/env bash
set -euo pipefail

processes="${1:-5}"
runs="${2:-5}"

if ! [[ "${processes}" =~ ^[1-9][0-9]*$ && "${runs}" =~ ^[1-9][0-9]*$ ]]; then
  echo "usage: bench/engine_query_distribution.sh [fresh-processes] [runs-per-process]" >&2
  exit 2
fi

metrics='^(first_exact_after_flush|exact_raw|narrow_raw|wide_raw|selective_regex_raw|selective_regex_discovery|selective_negative_raw|scalar_aggregate|bucketed_avg_10s|latest_exact|latest_multi)$'

echo "# benchmark=engine-query-fresh-process-distribution"
echo "# fresh_processes_per_engine=${processes}"
echo "# runs_per_process=${runs}"
echo "process,engine,populate_points_per_second,flush_us,database_bytes,wide_peak_multiple,metric,median_us,p95_us,min_us,max_us,runs,result_a,result_b"

for engine in rust libsql; do
  for process in $(seq 1 "${processes}"); do
    output=$(MIX_ENV=test mix run --no-compile bench/engine_query_bench.exs \
      --engine "${engine}" --runs "${runs}")

    populate=$(sed -n 's/^# populate_points_per_second=//p' <<<"${output}")
    flush=$(sed -n 's/^# flush_us=//p' <<<"${output}")
    bytes=$(sed -n 's/^# database_bytes=//p' <<<"${output}")
    peak=$(sed -n 's/^# wide_query_process_peak_multiple=//p' <<<"${output}")

    awk -F, \
      -v process="${process}" \
      -v engine="${engine}" \
      -v populate="${populate}" \
      -v flush="${flush}" \
      -v bytes="${bytes}" \
      -v peak="${peak}" \
      -v metrics="${metrics}" \
      '$1 ~ metrics {print process "," engine "," populate "," flush "," bytes "," peak "," $0}' \
      <<<"${output}"
  done
done
