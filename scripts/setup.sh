#!/usr/bin/env bash
# Verifies prerequisites and prepares the Kubernetes namespace. Safe to rerun.
source "$(dirname "$0")/lib/common.sh"

missing=0
for tool in docker kubectl helm mvn python3 jq nc; do
  if command -v "$tool" >/dev/null; then
    log "found $tool"
  else
    log "MISSING: $tool"; missing=1
  fi
done
JAVA="$(java25_home)/bin/java"
"$JAVA" -version 2>&1 | head -1 | sed 's/^/java: /' >&2
[[ $missing -eq 0 ]] || die "install the missing tools first (see README.md)"

kubectl config get-contexts "$KUBE_CONTEXT" >/dev/null 2>&1 \
  || die "kube context '$KUBE_CONTEXT' not found; enable Kubernetes in OrbStack or set AGGBENCH_KUBE_CONTEXT"
kubectl --context "$KUBE_CONTEXT" get nodes -o wide
docker info --format 'docker: {{.ServerVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes' \
  || die "docker daemon not reachable"

nodes=$(kubectl --context "$KUBE_CONTEXT" get nodes --no-headers | wc -l | tr -d ' ')
cpu=$(kubectl --context "$KUBE_CONTEXT" get nodes -o jsonpath='{.items[0].status.allocatable.cpu}')
mem=$(kubectl --context "$KUBE_CONTEXT" get nodes -o jsonpath='{.items[0].status.allocatable.memory}')
log "cluster: $nodes node(s), allocatable cpu=$cpu memory=$mem"
log "recommended: >= 8 CPUs and >= 14 GiB for the local profile; other workloads on the cluster add noise"

kubectl --context "$KUBE_CONTEXT" get namespace "$NAMESPACE" >/dev/null 2>&1 \
  || kubectl --context "$KUBE_CONTEXT" create namespace "$NAMESPACE"
mkdir -p "$RESULTS_DIR/runs"
log "setup complete (namespace $NAMESPACE)"
