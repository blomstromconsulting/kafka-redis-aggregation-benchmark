#!/usr/bin/env bash
# Re-runs the independent verifier for one saved run whose raw rows are still in PostgreSQL.
#   ./scripts/verify-run.sh <run-id> [results-dir]
source "$(dirname "$0")/lib/common.sh"
run_id="${1:?usage: verify-run.sh <run-id> [results-dir]}"
out="${2:-$RESULTS_DIR/runs/$run_id}"
mkdir -p "$out"
pg_pid=$(port_forward svc/postgres 15432 5432)
trap 'kill $pg_pid 2>/dev/null || true' EXIT
"$(java25_home)/bin/java" -jar "$ROOT/correctness-verifier/target/correctness-verifier.jar" \
  --jdbc-url "jdbc:postgresql://localhost:15432/aggbench" --user "$(secret_value user)" \
  --password "$(secret_value password)" --run-id "$run_id" --out "$out" > "$out/verifier.log" 2>&1 || true
jq -c '{correctness: .correctness.status, failures: .correctness.failures, slo: .slo.overall}' "$out/verification.json"
