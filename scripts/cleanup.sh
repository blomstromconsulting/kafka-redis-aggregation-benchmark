#!/usr/bin/env bash
# Removes benchmark resources.
#   ./scripts/cleanup.sh          uninstall the current benchmark run and delete leftover benchmark topics
#   ./scripts/cleanup.sh --all    also uninstall the infrastructure and delete the namespace (PVCs included)
# Results in results/ are never deleted.
source "$(dirname "$0")/lib/common.sh"
ALL=false
[[ "${1:-}" == "--all" ]] && ALL=true

if h status "$BENCH_RELEASE" >/dev/null 2>&1; then
  log "uninstalling $BENCH_RELEASE"
  h uninstall "$BENCH_RELEASE" --wait --timeout 3m
fi
if k get pod kafka-0 >/dev/null 2>&1; then
  log "deleting benchmark topics"
  kafka_tool kafka-topics.sh --delete --topic 'aggbench\..*|aggbench-.*' >/dev/null 2>&1 || true
fi
if [[ "$ALL" == true ]]; then
  log "uninstalling infrastructure and deleting namespace $NAMESPACE (including persistent volumes)"
  h uninstall "$INFRA_RELEASE" --wait --timeout 5m || true
  kubectl --context "$KUBE_CONTEXT" delete namespace "$NAMESPACE" --wait=true || true
  kubectl --context "$KUBE_CONTEXT" delete clusterrole,clusterrolebinding "aggbench-prometheus-$NAMESPACE" --ignore-not-found
fi
log "cleanup complete"
