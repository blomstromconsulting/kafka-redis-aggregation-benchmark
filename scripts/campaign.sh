#!/usr/bin/env bash
# The benchmark campaign used for docs/RESULTS.md: first every local-profile scenario once per variant (so that
# every scenario has data early), then repetitions 2-3 of the core scenarios S1-S5.
set -uo pipefail
cd "$(dirname "$0")/.."
./scripts/benchmark.sh --all --repetitions 1 \
  --scenario steady --scenario rolling --scenario abrupt-pod --scenario cold-pod --scenario rescheduling \
  --scenario abrupt-container --scenario state-1s --scenario state-10s --scenario state-60s --scenario slow-ml \
  --scenario correctness --scenario redis-restart --scenario redis-loss --scenario redis-failover --scenario smoke
./scripts/benchmark.sh --suite core --all --repetitions 2 --repetition-start 2
./scripts/report.sh
echo CAMPAIGN-DONE
