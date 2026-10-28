#!/usr/bin/env bash
# Automated tests.
#   ./scripts/test.sh            unit + integration tests (Maven; Testcontainers needs Docker)
#   ./scripts/test.sh --k8s      additionally a Kubernetes smoke test of A1, A2 and B with verification
source "$(dirname "$0")/lib/common.sh"
K8S=false
[[ "${1:-}" == "--k8s" ]] && K8S=true

export JAVA_HOME="$(java25_home)"
log "unit and integration tests"
(cd "$ROOT" && mvn -B -q install)
log "report generator self-check"
python3 -m py_compile "$ROOT/scripts/report.py" "$ROOT/scripts/lib/prom_export.py" "$ROOT/scripts/lib/gc_pauses.py" \
  "$ROOT/scripts/analysis/window_sensitivity.py" \
  "$ROOT/scripts/analysis/late_event_causes.py" \
  "$ROOT/scripts/analysis/disruption_impact.py"
for script in "$ROOT"/scripts/*.sh "$ROOT"/scripts/lib/*.sh; do bash -n "$script"; done
helm lint "$ROOT/helm/benchmark" >/dev/null
helm lint "$ROOT/helm/infrastructure" --set-file prometheus.config="$ROOT/monitoring/prometheus/prometheus.yml" >/dev/null

if [[ "$K8S" == true ]]; then
  "$ROOT/scripts/build.sh" --skip-tests
  "$ROOT/scripts/deploy.sh"
  before=$(ls "$RESULTS_DIR/runs" 2>/dev/null | wc -l)
  "$ROOT/scripts/benchmark.sh" --scenario smoke --all --repetitions 1
  failed=0
  for dir in $(ls -td "$RESULTS_DIR"/runs/smoke-* | head -3); do
    status=$(jq -r '.correctness.status' "$dir/verification.json" 2>/dev/null || echo MISSING)
    log "$(basename "$dir"): correctness $status"
    [[ "$status" == "PASS" ]] || failed=1
  done
  [[ $failed -eq 0 ]] || die "Kubernetes smoke test failed"
fi
log "all tests passed"
