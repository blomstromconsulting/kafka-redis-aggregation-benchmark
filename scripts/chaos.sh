#!/usr/bin/env bash
# Fault injection for one run. Waits until (measurement start + --at seconds), injects the fault, records a
# FAULT_* timeline event immediately before acting, and waits for Kubernetes to restore the desired state.
#
#   ./scripts/chaos.sh --run-id ID --fault F [--at S] [--interval S] [--count N]
#
# Faults (application):  rolling | kill-pod | kill-container | delete-pod | reschedule
# Faults (dependencies): ml-slowdown | redis-restart | redis-loss | redis-failover | kafka-restart
#
# Distinctions that matter for the results:
#   kill-container  SIGKILL of the JVM; the SAME pod restarts the container and keeps its emptyDir (warm state).
#   kill-pod        SIGKILL of the JVM, then force delete: no shutdown hooks, no leave-group, a NEW pod with an
#                   EMPTY emptyDir. (A force delete alone is NOT abrupt: the kubelet still sends SIGTERM first.)
#   delete-pod      graceful delete (SIGTERM, leave group), NEW pod with an EMPTY emptyDir ("cold replacement").
#   rolling         kubectl rollout restart: every pod replaced one by one (maxSurge 1, maxUnavailable 0).
source "$(dirname "$0")/lib/common.sh"

RUN_ID="" FAULT="none" AT=60 INTERVAL=45 COUNT=1
SLOWDOWN_FACTOR="${ML_SLOWDOWN_FACTOR:-6}" SLOWDOWN_SECONDS="${ML_SLOWDOWN_SECONDS:-60}"
LOSS_WINDOW="${REDIS_LOSS_WINDOW_SECONDS:-5}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id) RUN_ID="$2"; shift 2 ;;
    --fault) FAULT="$2"; shift 2 ;;
    --at) AT="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --count) COUNT="$2"; shift 2 ;;
    *) die "unknown option $1" ;;
  esac
done
[[ -n "$RUN_ID" ]] || die "--run-id is required"
[[ "$FAULT" != "none" ]] || exit 0

wait_for_measure_offset() {
  local offset="$1" wait_s=""
  for _ in $(seq 1 600); do
    wait_s=$(psql_exec "SELECT round(measure_start_us / 1e6 + $offset - extract(epoch from clock_timestamp()), 1) FROM runs WHERE run_id = '$RUN_ID' AND measure_start_us IS NOT NULL" || true)
    [[ -n "$wait_s" ]] && break
    sleep 1
  done
  [[ -n "$wait_s" ]] || die "run $RUN_ID never started"
  if (( $(echo "$wait_s > 0" | bc -l) )); then
    log "chaos: waiting ${wait_s}s until measurement + ${offset}s"
    sleep "$wait_s"
  fi
}

wait_aggregators_ready() {
  local start=$SECONDS
  k rollout status deployment/aggregator --timeout=600s >/dev/null 2>&1 || log "chaos: rollout status timed out"
  record_event "$RUN_ID" "REPLACEMENT_READY" "{\"waitedSeconds\": $((SECONDS - start))}"
}

# Docker id of the pod's application container (OrbStack's Kubernetes uses the local Docker engine).
container_id_of() {
  local id
  id=$(k get pod "$1" -o jsonpath='{.status.containerStatuses[0].containerID}' | sed 's#.*://##')
  docker inspect "$id" >/dev/null 2>&1 \
    || die "abrupt kills need the Kubernetes container runtime to be the local Docker engine (OrbStack)"
  echo "$id"
}

oldest_aggregator_pod() {
  k get pods -l app=aggregator --field-selector=status.phase=Running --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{.items[0].metadata.name}'
}

ml_set_slowdown() {
  local factor="$1" pod profile
  pod=$(k get pods -l app=ml-simulator -o jsonpath='{.items[0].metadata.name}')
  profile=$(k exec "$pod" -- curl -s localhost:8080/admin/profile | jq -c ".slowdownFactor = $factor")
  k exec "$pod" -- curl -s -XPOST -H 'Content-Type: application/json' -d "$profile" localhost:8080/admin/profile >/dev/null
}

inject() {
  local pod container_id
  case "$FAULT" in
    rolling)
      record_event "$RUN_ID" "FAULT_ROLLING_RESTART" '{}'
      k rollout restart deployment/aggregator >/dev/null
      wait_aggregators_ready ;;
    kill-pod)
      pod=$(oldest_aggregator_pod)
      container_id=$(container_id_of "$pod")
      record_event "$RUN_ID" "FAULT_POD_KILL" "{\"pod\": \"$pod\", \"mechanism\": \"SIGKILL + force delete\"}"
      docker kill --signal KILL "$container_id" >/dev/null
      k delete pod "$pod" --grace-period=0 --force --wait=false >/dev/null 2>&1
      wait_aggregators_ready ;;
    delete-pod)
      pod=$(oldest_aggregator_pod)
      record_event "$RUN_ID" "FAULT_POD_DELETE" "{\"pod\": \"$pod\"}"
      k delete pod "$pod" --wait=false >/dev/null
      wait_aggregators_ready ;;
    kill-container)
      pod=$(oldest_aggregator_pod)
      container_id=$(container_id_of "$pod")
      record_event "$RUN_ID" "FAULT_CONTAINER_KILL" "{\"pod\": \"$pod\", \"signal\": \"KILL\"}"
      docker kill --signal KILL "$container_id" >/dev/null
      sleep 5
      wait_aggregators_ready ;;
    ml-slowdown)
      record_event "$RUN_ID" "FAULT_ML_SLOWDOWN" "{\"factor\": $SLOWDOWN_FACTOR, \"seconds\": $SLOWDOWN_SECONDS}"
      ml_set_slowdown "$SLOWDOWN_FACTOR"
      sleep "$SLOWDOWN_SECONDS"
      ml_set_slowdown 1
      record_event "$RUN_ID" "ML_SLOWDOWN_END" '{}' ;;
    redis-restart)
      record_event "$RUN_ID" "FAULT_REDIS_RESTART" '{"pod": "redis-0", "graceful": true}'
      k delete pod redis-0 --wait=false >/dev/null
      sleep 3
      k rollout status statefulset/redis --timeout=300s >/dev/null
      record_event "$RUN_ID" "REDIS_READY" '{}' ;;
    redis-loss)
      record_event "$RUN_ID" "REDIS_LOSS_WINDOW_START" "{\"seconds\": $LOSS_WINDOW}"
      k exec redis-0 -c redis -- redis-cli CONFIG SET appendonly no >/dev/null
      sleep "$LOSS_WINDOW"
      record_event "$RUN_ID" "FAULT_REDIS_CRASH_WITH_LOSS" "{\"lostWindowSeconds\": $LOSS_WINDOW}"
      k exec redis-0 -c redis -- redis-cli SHUTDOWN NOSAVE >/dev/null 2>&1 || true
      sleep 3
      k rollout status statefulset/redis --timeout=300s >/dev/null
      for _ in $(seq 1 60); do k exec redis-0 -c redis -- redis-cli ping 2>/dev/null | grep -q PONG && break; sleep 1; done
      record_event "$RUN_ID" "REDIS_READY" '{}' ;;
    redis-failover)
      local master
      master=$(k exec redis-sentinel-0 -- redis-cli -p 26379 SENTINEL get-master-addr-by-name aggbench | head -1 | cut -d. -f1)
      record_event "$RUN_ID" "FAULT_REDIS_PRIMARY_KILL" "{\"pod\": \"$master\"}"
      k delete pod "$master" --grace-period=0 --force --wait=false >/dev/null 2>&1
      for _ in $(seq 1 120); do
        local now
        now=$(k exec redis-sentinel-0 -- redis-cli -p 26379 SENTINEL get-master-addr-by-name aggbench | head -1 | cut -d. -f1)
        [[ "$now" != "$master" ]] && { record_event "$RUN_ID" "REDIS_FAILOVER_COMPLETE" "{\"newPrimary\": \"$now\"}"; break; }
        sleep 1
      done ;;
    kafka-restart)
      record_event "$RUN_ID" "FAULT_KAFKA_BROKER_RESTART" '{"pod": "kafka-0"}'
      k delete pod kafka-0 --wait=false >/dev/null
      sleep 3
      k rollout status statefulset/kafka --timeout=300s >/dev/null
      record_event "$RUN_ID" "KAFKA_READY" '{}' ;;
    *) die "unknown fault $FAULT" ;;
  esac
}

if [[ "$FAULT" == "reschedule" ]]; then
  wait_for_measure_offset "$AT"
  for i in $(seq 1 "$COUNT"); do
    FAULT=delete-pod
    log "chaos: rescheduling $i/$COUNT"
    inject
    FAULT=reschedule
    [[ $i -lt $COUNT ]] && wait_for_measure_offset $((AT + i * INTERVAL))
  done
else
  wait_for_measure_offset "$AT"
  log "chaos: injecting $FAULT"
  inject
fi
log "chaos: done ($FAULT)"
