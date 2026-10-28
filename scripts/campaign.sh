#!/usr/bin/env bash
# The final benchmark campaign: every local-profile scenario once per variant (so that every scenario has data
# early), then repetitions 2-3 of the core scenarios S1-S5. With both builds (default), each scenario runs with the
# JVM build and then with the native build before the next one starts, so drift over the ~15 h affects both alike.
# JVM runs go to results/ (docs/RESULTS.md), native runs to results/native/ (its own RESULTS.md and report.html).
#   ./scripts/campaign.sh [--build jvm|native|both]
# Redis Sentinel failover and the extended suite (stress, 300 s state window, Kafka restart) are not part of it.
set -uo pipefail
cd "$(dirname "$0")/.."

BUILDS=(jvm native)
if [[ $# -gt 0 ]]; then
  [[ "$1" == --build && $# -eq 2 ]] || { echo "usage: $0 [--build jvm|native|both]" >&2; exit 1; }
  case "$2" in
    jvm|native) BUILDS=("$2") ;;
    both) ;;
    *) echo "unknown build $2" >&2; exit 1 ;;
  esac
fi

results_dir() { [[ "$1" == native ]] && echo "$PWD/results/native" || echo "$PWD/results"; }

run() {  # build, benchmark.sh arguments...
  local build="$1"; shift
  AGGBENCH_RESULTS="$(results_dir "$build")" ./scripts/benchmark.sh --build "$build" "$@" || echo "FAILED: $build $*"
}

for scenario in steady rolling abrupt-pod cold-pod rescheduling abrupt-container state-1s state-10s state-60s slow-ml \
                correctness redis-restart redis-loss smoke; do
  for build in "${BUILDS[@]}"; do run "$build" --all --repetitions 1 --scenario "$scenario"; done
done
for rep in 2 3; do
  for scenario in steady rolling abrupt-pod cold-pod rescheduling; do
    for build in "${BUILDS[@]}"; do run "$build" --all --repetitions 1 --repetition-start "$rep" --scenario "$scenario"; done
  done
done
for build in "${BUILDS[@]}"; do AGGBENCH_RESULTS="$(results_dir "$build")" ./scripts/report.sh; done
echo CAMPAIGN-DONE
