#!/usr/bin/env bash
# Installs or upgrades the shared infrastructure (Kafka, Redis, PostgreSQL, Prometheus, Grafana) with Helm.
#   ./scripts/deploy.sh            local lightweight profile
#   ./scripts/deploy.sh --ha       3 Kafka brokers + Redis Sentinel (needs more memory)
source "$(dirname "$0")/lib/common.sh"

VALUES=(-f "$ROOT/helm/infrastructure/values.yaml")
PROFILE=local
while [[ $# -gt 0 ]]; do
  case "$1" in
    --ha) VALUES+=(-f "$ROOT/helm/infrastructure/values-ha.yaml"); PROFILE=ha; shift ;;
    --no-grafana) VALUES+=(--set grafana.enabled=false); shift ;;
    *) die "unknown option $1 (supported: --ha, --no-grafana)" ;;
  esac
done

kubectl --context "$KUBE_CONTEXT" get namespace "$NAMESPACE" >/dev/null 2>&1 || "$ROOT/scripts/setup.sh"

log "installing infrastructure ($PROFILE profile) into namespace $NAMESPACE"
h upgrade --install "$INFRA_RELEASE" "$ROOT/helm/infrastructure" "${VALUES[@]}" \
  --set-file prometheus.config="$ROOT/monitoring/prometheus/prometheus.yml" \
  --set-file grafana.dashboard="$ROOT/monitoring/grafana/aggbench-dashboard.json" \
  --wait --timeout 10m

log "waiting for Kafka, Redis and PostgreSQL"
k rollout status statefulset/kafka --timeout=5m
k rollout status statefulset/redis --timeout=5m
k rollout status statefulset/postgres --timeout=5m

# The features/configs below must be effective; fail loudly if they are not.
features=$(kafka_tool kafka-features.sh describe) || die "could not read Kafka features"
grep -E 'streams.version|group.version' <<< "$features" >&2
grep -Eq 'Feature: streams.version.*FinalizedVersionLevel: [1-9]' <<< "$features" \
  || die "Kafka feature streams.version is not enabled; the streams rebalance protocol (A1) cannot run"
log "infrastructure ready. Grafana: kubectl -n $NAMESPACE port-forward svc/grafana 3000  -> http://localhost:3000"
