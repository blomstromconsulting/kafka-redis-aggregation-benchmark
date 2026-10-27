#!/usr/bin/env bash
# Runs benchmark scenarios for one or more variants, sequentially, each run fully isolated.
#
#   ./scripts/benchmark.sh --scenario smoke --all
#   ./scripts/benchmark.sh --scenario rolling --variant a1 --variant b --repetitions 5
#   ./scripts/benchmark.sh --suite full --all
#
# Options (override the scenario file):
#   --scenario NAME (repeatable) | --suite smoke|core|full|extended
#   --variant a1|a2|b (repeatable) | --all
#   --repetitions N (default 3)      --repetition-start K (default 1; adds repetitions K..K+N-1 later)
#   --seed N (default 42)
#   --rate N  --duration S  --warmup S  --replicas N  --partitions N
#   --payload-a B  --payload-b B  --payload-c B  --window-ms MS  --window-distribution uniform|fixed|exponential
#   --inference-latency-ms MS  --inference-percent P  --ml-profile fixed|normal|lognormal-tail
#   --fault F  --fault-at S  --fault-interval S  --fault-count N
#   --keep-db        keep raw per-action rows in PostgreSQL after verification (large)
#   --dry-run        print the plan only
#
# Every run: fresh topics/groups/Redis keys/run id, a fresh deployment of the aggregator and downstream
# services, the same seed and workload for every variant, a startup protocol verification, independent
# verification afterwards, and teardown. Variant order is rotated per repetition to spread drift evenly.
source "$(dirname "$0")/lib/common.sh"
source "$ROOT/scenarios/suites.env"

SCENARIOS=() VARIANTS=() REPETITIONS=3 REP_START=1 SEED=42 KEEP_DB=false DRY_RUN=false OVERRIDE_KEYS=""
# Command-line overrides are kept as O_<KEY> variables (macOS ships bash 3.2: no associative arrays).
override() { printf -v "O_$1" '%s' "$2"; OVERRIDE_KEYS="$OVERRIDE_KEYS $1"; }
while [[ $# -gt 0 ]]; do
  case "$1" in
    --scenario) SCENARIOS+=("$2"); shift 2 ;;
    --suite) suite_var="SUITE_$2"; [[ -n "${!suite_var:-}" ]] || die "unknown suite $2"; read -r -a s <<< "${!suite_var}"; SCENARIOS+=("${s[@]}"); shift 2 ;;
    --variant) VARIANTS+=("$2"); shift 2 ;;
    --all) VARIANTS=(a1 a2 b); shift ;;
    --repetitions) REPETITIONS="$2"; shift 2 ;;
    --repetition-start) REP_START="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --rate) override EVENT_RATE "$2"; shift 2 ;;
    --duration) override DURATION_SECONDS "$2"; shift 2 ;;
    --warmup) override WARMUP_SECONDS "$2"; shift 2 ;;
    --replicas) override REPLICAS "$2"; shift 2 ;;
    --partitions) override PARTITIONS "$2"; shift 2 ;;
    --payload-a) override PAYLOAD_A "$2"; shift 2 ;;
    --payload-b) override PAYLOAD_B "$2"; shift 2 ;;
    --payload-c) override PAYLOAD_C "$2"; shift 2 ;;
    --window-ms) override CORRELATION_WINDOW_MS "$2"; shift 2 ;;
    --window-distribution) override WINDOW_DISTRIBUTION "$2"; shift 2 ;;
    --inference-latency-ms) override ML_LATENCY_MS "$2"; shift 2 ;;
    --inference-percent) override INFERENCE_PERCENT "$2"; shift 2 ;;
    --ml-profile) override ML_PROFILE "$2"; shift 2 ;;
    --fault) override FAULT "$2"; shift 2 ;;
    --fault-at) override FAULT_AT "$2"; shift 2 ;;
    --fault-interval) override FAULT_INTERVAL "$2"; shift 2 ;;
    --fault-count) override FAULT_COUNT "$2"; shift 2 ;;
    --keep-db) KEEP_DB=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    *) die "unknown option $1 (see --help)" ;;
  esac
done
[[ ${#SCENARIOS[@]} -gt 0 ]] || die "no scenario given (--scenario or --suite)"
[[ ${#VARIANTS[@]} -gt 0 ]] || die "no variant given (--variant or --all)"
for v in "${VARIANTS[@]}"; do [[ "$v" =~ ^(a1|a2|b)$ ]] || die "unknown variant $v"; done
for s in "${SCENARIOS[@]}"; do [[ -f "$ROOT/scenarios/$s.env" ]] || die "unknown scenario $s"; done

VERIFIER_JAR="$ROOT/correctness-verifier/target/correctness-verifier.jar"
[[ -f "$VERIFIER_JAR" ]] || die "verifier not built; run ./scripts/build.sh"
k get statefulset kafka >/dev/null 2>&1 || die "infrastructure not deployed; run ./scripts/deploy.sh"
INFRA_PROFILE=$(h get values "$INFRA_RELEASE" -a -o json | jq -r '.profile')
KAFKA_RF=$(h get values "$INFRA_RELEASE" -a -o json | jq -r '.kafka.replicationFactor')
KAFKA_MIN_ISR=$(h get values "$INFRA_RELEASE" -a -o json | jq -r '.kafka.minInsyncReplicas')
GROUP_SESSION_TIMEOUT_MS=$(h get values "$INFRA_RELEASE" -a -o json | jq -r '.kafka.groupSessionTimeoutMs')
GROUP_HEARTBEAT_INTERVAL_MS=$(h get values "$INFRA_RELEASE" -a -o json | jq -r '.kafka.groupHeartbeatIntervalMs')

# Loads a scenario into P_* variables: defaults < scenario file < command-line overrides.
load_scenario() {
  local scenario="$1"
  P_EVENT_RATE=100 P_WARMUP_SECONDS=30 P_RAMP_SECONDS=15 P_DURATION_SECONDS=60 P_REPLICAS=3 P_PARTITIONS=12
  P_PAYLOAD_A=200 P_PAYLOAD_B=200 P_PAYLOAD_C=400 P_CORRELATION_WINDOW_MS=1000 P_WINDOW_DISTRIBUTION=uniform
  P_OUT_OF_ORDER_PERCENT=10 P_DUPLICATE_PERCENT=2 P_DUPLICATE_DELAY_MAX_MS=2000 P_MISSING_PERCENT=1 P_STALE_PERCENT=0
  P_INFERENCE_PERCENT=100 P_ML_PROFILE=fixed P_ML_LATENCY_MS=50 P_ML_ERROR_RATE=0
  P_PERSISTENCE_ERROR_BEFORE_RATE=0 P_PERSISTENCE_ERROR_AFTER_COMMIT_RATE=0
  P_FAULT=none P_FAULT_AT=60 P_FAULT_INTERVAL=45 P_FAULT_COUNT=1 P_ML_SLOWDOWN_FACTOR=6 P_ML_SLOWDOWN_SECONDS=60
  P_REDIS_LOSS_WINDOW_SECONDS=5 P_REDIS_URI="redis://redis:6379" P_REDIS_REPLAY_WINDOW=60s
  P_VARIANTS_APPLICABLE="a1 a2 b" P_REQUIRES_PROFILE="" P_DESCRIPTION="$scenario"
  local line key value
  while IFS= read -r line; do
    [[ "$line" =~ ^[A-Z_]+= ]] || continue
    key="${line%%=*}"; value="${line#*=}"; value="${value%\"}"; value="${value#\"}"
    printf -v "P_$key" '%s' "$value"
  done < "$ROOT/scenarios/$scenario.env"
  for key in $OVERRIDE_KEYS; do
    value="O_$key"
    printf -v "P_$key" '%s' "${!value}"
  done
}

teardown_run() {
  local run_id="$1" variant="$2"
  if h status "$BENCH_RELEASE" >/dev/null 2>&1; then
    h uninstall "$BENCH_RELEASE" --wait --timeout 3m >/dev/null 2>&1 || true
  fi
  k wait --for=delete pod -l app.kubernetes.io/part-of=aggbench,app=aggregator --timeout=120s >/dev/null 2>&1 || true
  [[ -z "$run_id" ]] && return 0
  local group="aggbench-$variant-$run_id"
  kafka_tool kafka-topics.sh --delete --topic "aggbench\\.$run_id\\..*|$group-.*" >/dev/null 2>&1 || true
  if [[ "$variant" == "a1" ]]; then
    kafka_tool kafka-streams-groups.sh --delete --group "$group" >/dev/null 2>&1 || true
  else
    kafka_tool kafka-consumer-groups.sh --delete --group "$group" >/dev/null 2>&1 || true
  fi
  kafka_tool kafka-consumer-groups.sh --delete --group "$group-intents" >/dev/null 2>&1 || true
}

reset_shared_state() {
  # Redis is used only by variant B; it is flushed before every run so memory and keyspace start empty.
  k exec redis-0 -c redis -- redis-cli CONFIG SET appendonly yes >/dev/null
  k exec redis-0 -c redis -- redis-cli FLUSHALL >/dev/null
  [[ "$(k exec redis-0 -c redis -- redis-cli DBSIZE)" == "0" ]] || die "Redis reset failed"
  # Same database state for every run: flush dirty pages now rather than during the measurement window.
  psql_exec "CHECKPOINT" >/dev/null
}

create_topics() {
  local run_id="$1" variant="$2"
  kafka_tool kafka-topics.sh --create --topic "aggbench.$run_id.ingress" --partitions "$P_PARTITIONS" \
    --replication-factor "$KAFKA_RF" --config message.timestamp.type=LogAppendTime \
    --config min.insync.replicas="$KAFKA_MIN_ISR" >/dev/null
  if [[ "$variant" != "b" ]]; then
    kafka_tool kafka-topics.sh --create --topic "aggbench.$run_id.actions" --partitions "$P_PARTITIONS" \
      --replication-factor "$KAFKA_RF" --config min.insync.replicas="$KAFKA_MIN_ISR" >/dev/null
  fi
  if [[ "$variant" == "a1" ]]; then
    # A1: standby replicas and membership timing are GROUP-level configs of the streams group (client-side
    # values are ignored). Kafka 4.2 reports the built-in streams defaults (45 s / 5 s / 3 s) for a new streams
    # group even when the broker-level group.streams.* values differ, so they are set explicitly here and
    # verified by every pod at startup.
    kafka_tool kafka-configs.sh --entity-type groups --entity-name "aggbench-a1-$run_id" --alter \
      --add-config "streams.num.standby.replicas=1,streams.session.timeout.ms=$GROUP_SESSION_TIMEOUT_MS,streams.heartbeat.interval.ms=$GROUP_HEARTBEAT_INTERVAL_MS,streams.initial.rebalance.delay.ms=0" >/dev/null
  fi
}

helm_values() {
  local run_id="$1" variant="$2" scenario="$3" rep="$4"
  cat <<EOF
runId: "$run_id"
variant: "$variant"
images: {tag: "$IMAGE_TAG"}
kafka: {partitions: $P_PARTITIONS, replicationFactor: $KAFKA_RF}
aggregator:
  replicas: $P_REPLICAS
  groupSessionTimeoutMs: $GROUP_SESSION_TIMEOUT_MS
  groupHeartbeatIntervalMs: $GROUP_HEARTBEAT_INTERVAL_MS
  redis: {uri: "$P_REDIS_URI", replayWindow: "$P_REDIS_REPLAY_WINDOW"}
persistence:
  faults: {errorBeforeRate: $P_PERSISTENCE_ERROR_BEFORE_RATE, errorAfterCommitRate: $P_PERSISTENCE_ERROR_AFTER_COMMIT_RATE}
ml: {profile: "$P_ML_PROFILE", latencyMs: $P_ML_LATENCY_MS, errorRate: $P_ML_ERROR_RATE}
generator:
  scenario: "$scenario"
  repetition: $rep
  seed: $SEED
  eventRate: $P_EVENT_RATE
  warmupSeconds: $P_WARMUP_SECONDS
  rampSeconds: $P_RAMP_SECONDS
  durationSeconds: $P_DURATION_SECONDS
  payloadSizeA: $P_PAYLOAD_A
  payloadSizeB: $P_PAYLOAD_B
  payloadSizeC: $P_PAYLOAD_C
  correlationWindowMs: $P_CORRELATION_WINDOW_MS
  windowDistribution: "$P_WINDOW_DISTRIBUTION"
  outOfOrderPercent: $P_OUT_OF_ORDER_PERCENT
  duplicatePercent: $P_DUPLICATE_PERCENT
  duplicateDelayMaxMs: $P_DUPLICATE_DELAY_MAX_MS
  missingPercent: $P_MISSING_PERCENT
  stalePercent: $P_STALE_PERCENT
  inferencePercent: $P_INFERENCE_PERCENT
EOF
}

wait_config_verified() {
  local deadline=$((SECONDS + 240)) pod status all_ok
  while (( SECONDS < deadline )); do
    all_ok=true
    for pod in $(aggregator_pods); do
      status=$(pod_get "$pod" /status 2>/dev/null | jq -r '.configVerification' 2>/dev/null || echo PENDING)
      [[ "$status" == "FAILED" ]] && die "protocol/config verification FAILED on $pod (see its log)"
      [[ "$status" == "VERIFIED" ]] || all_ok=false
    done
    [[ "$all_ok" == true && -n "$(aggregator_pods)" ]] && return 0
    sleep 3
  done
  die "aggregators did not verify their group protocol within 240 s"
}

collect_artifacts() {
  local run_id="$1" out="$2"
  mkdir -p "$out/logs"
  for pod in $(k get pods -l "aggbench/run=$run_id" -o jsonpath='{.items[*].metadata.name}'); do
    k logs "$pod" --all-containers > "$out/logs/$pod.log" 2>&1 || true
    k logs "$pod" --all-containers --previous > "$out/logs/$pod.previous.log" 2>/dev/null || rm -f "$out/logs/$pod.previous.log"
  done
  k get events --sort-by=.lastTimestamp > "$out/logs/kubernetes-events.txt" 2>&1 || true
  for pod in $(aggregator_pods); do pod_get "$pod" /status > "$out/status-$pod.json" 2>/dev/null || true; done
}

run_one() {
  local scenario="$1" variant="$2" rep="$3"
  load_scenario "$scenario"
  if [[ " $P_VARIANTS_APPLICABLE " != *" $variant "* ]]; then
    log "skip $scenario/$variant (scenario applies to: $P_VARIANTS_APPLICABLE)"; return 0
  fi
  if [[ -n "$P_REQUIRES_PROFILE" && "$P_REQUIRES_PROFILE" != "$INFRA_PROFILE" ]]; then
    log "NOT RUN $scenario/$variant: requires the '$P_REQUIRES_PROFILE' infrastructure profile (deployed: $INFRA_PROFILE)"
    mkdir -p "$RESULTS_DIR/not-run"; echo "$scenario,$variant,requires profile $P_REQUIRES_PROFILE" >> "$RESULTS_DIR/not-run/not-run.csv"
    return 0
  fi
  local run_id out
  run_id=$(printf '%s-%s-r%s-%s' "$scenario" "$variant" "$rep" "$(date +%m%d%H%M%S)" | tr 'A-Z' 'a-z')
  out="$RESULTS_DIR/runs/$run_id"
  if [[ "$DRY_RUN" == true ]]; then
    log "[dry-run] $run_id rate=$P_EVENT_RATE warmup=$P_WARMUP_SECONDS duration=$P_DURATION_SECONDS fault=$P_FAULT"; return 0
  fi
  mkdir -p "$out"
  log "=== run $run_id: $P_DESCRIPTION [effective: rate=$P_EVENT_RATE warmup=$P_WARMUP_SECONDS duration=$P_DURATION_SECONDS fault=$P_FAULT@$P_FAULT_AT]"
  helm_values "$run_id" "$variant" "$scenario" "$rep" > "$out/values.yaml"
  cp "$ROOT/scenarios/$scenario.env" "$out/scenario.env"
  echo "infraProfile=$INFRA_PROFILE seed=$SEED" > "$out/environment.txt"
  kubectl --context "$KUBE_CONTEXT" get nodes -o wide >> "$out/environment.txt"

  teardown_run "" "$variant"
  reset_shared_state
  create_topics "$run_id" "$variant"

  h upgrade --install "$BENCH_RELEASE" "$ROOT/helm/benchmark" -f "$out/values.yaml" --set generator.enabled=false \
    --wait --timeout 6m >/dev/null || { collect_artifacts "$run_id" "$out"; die "deployment of $run_id failed"; }
  wait_config_verified
  for pod in $(aggregator_pods); do pod_get "$pod" /status > "$out/startup-status-$pod.json"; done
  log "config verified; starting load"

  h upgrade "$BENCH_RELEASE" "$ROOT/helm/benchmark" -f "$out/values.yaml" --set generator.enabled=true >/dev/null
  local chaos_pid=""
  if [[ "$P_FAULT" != "none" ]]; then
    ML_SLOWDOWN_FACTOR=$P_ML_SLOWDOWN_FACTOR ML_SLOWDOWN_SECONDS=$P_ML_SLOWDOWN_SECONDS \
    REDIS_LOSS_WINDOW_SECONDS=$P_REDIS_LOSS_WINDOW_SECONDS \
      "$ROOT/scripts/chaos.sh" --run-id "$run_id" --fault "$P_FAULT" --at "$P_FAULT_AT" \
      --interval "$P_FAULT_INTERVAL" --count "$P_FAULT_COUNT" > "$out/chaos.log" 2>&1 &
    chaos_pid=$!
  fi

  local timeout=$((P_WARMUP_SECONDS + P_DURATION_SECONDS + 900))
  local job="job/load-generator-$run_id" state=""
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    state=$(k get "$job" -o jsonpath='{.status.conditions[?(@.status=="True")].type}' 2>/dev/null || true)
    [[ "$state" == *Complete* || "$state" == *Failed* ]] && break
    sleep 5
  done
  log "load generator finished: ${state:-TIMEOUT}"
  [[ -n "$chaos_pid" ]] && { wait "$chaos_pid" || log "chaos script reported an error (see chaos.log)"; }
  collect_artifacts "$run_id" "$out"

  local prom_pid pg_pid
  prom_pid=$(port_forward svc/prometheus 19090 9090)
  python3 "$ROOT/scripts/lib/prom_export.py" --url http://localhost:19090 --run-id "$run_id" \
    --db-cmd "$ROOT/scripts/lib/run_window.sh" --out "$out/resources.json" || log "prometheus export failed"
  kill "$prom_pid" 2>/dev/null || true

  pg_pid=$(port_forward svc/postgres 15432 5432)
  local verify_status=0
  "$(java21_home)/bin/java" -jar "$VERIFIER_JAR" --jdbc-url "jdbc:postgresql://localhost:15432/aggbench" \
    --user "$(secret_value user)" --password "$(secret_value password)" --run-id "$run_id" --out "$out" \
    > "$out/verifier.log" 2>&1 || verify_status=$?
  kill "$pg_pid" 2>/dev/null || true
  log "verification exit=$verify_status: $(jq -c '{correctness: .correctness.status, slo: .slo.overall, steady_p9995_final: .latency.steady.FINAL.p99_95}' "$out/verification.json" 2>/dev/null || tail -3 "$out/verifier.log")"

  if [[ "$KEEP_DB" != true && -f "$out/verification.json" ]]; then
    psql_exec "DELETE FROM action_receipts WHERE run_id = '$run_id'; DELETE FROM aggregates WHERE run_id = '$run_id';
               DELETE FROM manifest WHERE run_id = '$run_id'; DELETE FROM commit_batches WHERE run_id = '$run_id';" >/dev/null
    # Reclaim the deleted rows now, so that this vacuum does not run during the next measurement.
    psql_exec "VACUUM (ANALYZE) action_receipts, aggregates, manifest, commit_batches" >/dev/null
  fi
  teardown_run "$run_id" "$variant"
  echo "$run_id,$scenario,$variant,$rep,${state:-TIMEOUT},$verify_status" >> "$RESULTS_DIR/runs/index.csv"
}

mkdir -p "$RESULTS_DIR/runs"
log "plan: scenarios=[${SCENARIOS[*]}] variants=[${VARIANTS[*]}] repetitions=$REPETITIONS seed=$SEED profile=$INFRA_PROFILE"
for scenario in "${SCENARIOS[@]}"; do
  for rep in $(seq "$REP_START" $((REP_START + REPETITIONS - 1))); do
    n=${#VARIANTS[@]}
    for i in $(seq 0 $((n - 1))); do
      run_one "$scenario" "${VARIANTS[$(( (i + rep - 1) % n ))]}" "$rep"
    done
  done
done
log "all runs finished; generate the report with ./scripts/report.sh"
