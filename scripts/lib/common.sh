#!/usr/bin/env bash
# Shared helpers for the benchmark scripts. Source it; do not run it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE="${AGGBENCH_NAMESPACE:-aggbench}"
KUBE_CONTEXT="${AGGBENCH_KUBE_CONTEXT:-orbstack}"
RESULTS_DIR="${AGGBENCH_RESULTS:-$ROOT/results}"
INFRA_RELEASE="aggbench-infra"
BENCH_RELEASE="aggbench-run"
IMAGE_TAG="${AGGBENCH_IMAGE_TAG:-local}"
MODULES_WITH_IMAGES=(streams-aggregator redis-aggregator load-generator ml-simulator persistence-service)

log() { printf '%s %s\n' "$(date '+%H:%M:%S')" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

k() { kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" "$@"; }
h() { helm --kube-context "$KUBE_CONTEXT" -n "$NAMESPACE" "$@"; }

java21_home() {
  if [[ -n "${JAVA_HOME:-}" ]] && "$JAVA_HOME/bin/java" -version 2>&1 | grep -q '"21'; then
    echo "$JAVA_HOME"
  elif command -v /usr/libexec/java_home >/dev/null && /usr/libexec/java_home -v 21 >/dev/null 2>&1; then
    /usr/libexec/java_home -v 21
  else
    die "Java 21 not found (set JAVA_HOME to a JDK 21)"
  fi
}

# Runs a Kafka CLI tool inside the first broker, e.g. kafka_tool kafka-topics.sh --list
kafka_tool() {
  local tool="$1"; shift
  k exec kafka-0 -c kafka -- /opt/kafka/bin/"$tool" --bootstrap-server localhost:9092 "$@"
}

# Runs one SQL statement in the benchmark database and prints unaligned output.
psql_exec() {
  k exec postgres-0 -c postgres -- sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c "$1"' sh "$1"
}

# Records a benchmark timeline event (source=chaos). Fault events must be named FAULT_* (see the verifier).
record_event() {
  local run_id="$1" type="$2" detail="${3:-}"
  [[ -n "$detail" ]] || detail='{}'
  psql_exec "INSERT INTO bench_events (run_id, ts_us, source, type, detail) VALUES ('$run_id', (extract(epoch from clock_timestamp()) * 1000000)::bigint, 'chaos', '$type', '$detail'::jsonb)" >/dev/null
}

# GET a JSON endpoint of a pod through the API server proxy (no curl needed in the image).
pod_get() {
  local pod="$1" path="$2"
  kubectl --context "$KUBE_CONTEXT" get --raw "/api/v1/namespaces/$NAMESPACE/pods/$pod:8080/proxy$path"
}

aggregator_pods() {
  k get pods -l app=aggregator --field-selector=status.phase=Running -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort
}

secret_value() {
  k get secret aggbench-postgres -o jsonpath="{.data.$1}" | base64 --decode
}

# Starts a port-forward in the background and waits for the local port; prints the PID.
port_forward() {
  local target="$1" local_port="$2" remote_port="$3"
  k port-forward "$target" "$local_port:$remote_port" >/dev/null 2>&1 &
  local pid=$!
  for _ in $(seq 1 50); do
    if nc -z localhost "$local_port" 2>/dev/null; then echo "$pid"; return 0; fi
    sleep 0.2
  done
  kill "$pid" 2>/dev/null || true
  die "port-forward to $target did not become ready"
}

now_epoch() { python3 -c 'import time; print(int(time.time()))'; }
