# Kafka Streams vs. Redis aggregation benchmark

A reproducible benchmark comparing three ways to aggregate three event fragments (A, B, C) per correlation id
at 6,000 fragments/s with a 500 ms p99.95 end-to-end SLO, under frequent Kubernetes pod rescheduling:

| Variant | Aggregation state | Group protocol | Recovery of state |
|---|---|---|---|
| **A1** | Kafka Streams 4.2.2, RocksDB + changelog | `group.protocol=streams` (KIP-1071, broker-side assignment) | changelog restore; 1 standby (group config) |
| **A2** | identical code to A1 | `group.protocol=classic` (client-side HA assignor) | changelog restore; 1 standby (client config) + warmups |
| **B** | Redis 7.4 (atomic Lua function) | `group.protocol=consumer` (KIP-848) | stateless pods; Kafka replay after Redis loss |

All three run on Java 25 / Quarkus 3.40 (on the JVM, or optionally as native executables), execute the same
individual, cumulative (A+B) and final (A+B+C, ML inference, persistence) actions through the same idempotent
downstream services, and are verified by an independent checker. Nothing in this repository assumes a winner: results come only from `results/`.

## Prerequisites

- macOS with [OrbStack](https://orbstack.dev) and its Kubernetes enabled (context `orbstack`); ≥ 8 CPUs and
  ≥ 14 GiB assigned to OrbStack recommended
- Docker CLI, `kubectl`, Helm ≥ 3.14, JDK 25, Maven ≥ 3.9, Python 3.10+, `jq`

## Quick start (smoke test)

```bash
./scripts/setup.sh                                   # checks tools, creates namespace "aggbench"
./scripts/build.sh                                   # Maven build + tests, container images
./scripts/deploy.sh                                  # Kafka 4.2.2, Redis, PostgreSQL, Prometheus, Grafana
./scripts/benchmark.sh --scenario smoke --all --repetitions 1
./scripts/report.sh                                  # results/report.html, docs/RESULTS.md
```

Each run prints `correctness PASS/FAIL` and its SLO class; artefacts are written to `results/runs/<run-id>/`.

## Running the benchmark

```bash
./scripts/benchmark.sh --scenario steady --all                 # S1, 3 repetitions per variant
./scripts/benchmark.sh --scenario rolling --all                # S2 rolling upgrade
./scripts/benchmark.sh --scenario rescheduling --all           # S5 frequent pod rescheduling
./scripts/benchmark.sh --suite core --all                      # S1, S2, S3, S4, S5
./scripts/benchmark.sh --suite full --all --repetitions 5      # everything that fits the local profile
./scripts/campaign.sh                                         # final campaign: full suite x JVM and native (~15 h)
./scripts/benchmark.sh --scenario steady --variant b --rate 9000 --duration 180 --replicas 3 --partitions 12
./scripts/report.sh
./scripts/verify-run.sh <run-id>  # re-run the independent verifier (raw rows are kept if verification failed)
./scripts/analysis/window_sensitivity.py results/runs/<run>  # steady/failure split under two window rules
./scripts/analysis/disruption_impact.py --summary results/runs/<run> ...  # late events per rollout / pod failure
                                    # (--write saves <run>/disruption.json; benchmark.sh does it after every run)
./scripts/analysis/late_event_causes.py results/runs/<b-run> ...  # B: late events by pod and stage
./scripts/cleanup.sh            # remove the current run; --all also removes infrastructure and volumes
```

Native aggregators (ADR 0005): `./scripts/build.sh --native` additionally compiles A and B into native executables
with Oracle GraalVM 25 and the G1 collector (images tagged `local-native`); run them with `--build native`, into a
separate results directory so that JVM and native runs are never mixed (`campaign.sh` uses `results/` and
`results/native/`; earlier campaigns are in `results/archive/`):

```bash
AGGBENCH_RESULTS=$PWD/results/native ./scripts/benchmark.sh --scenario rolling --all --build native
AGGBENCH_RESULTS=$PWD/results/native ./scripts/report.sh
```

Scenarios live in `scenarios/*.env` (S1–S10, see `docs/BENCHMARK-METHODOLOGY.md`). Every option can be
overridden on the command line: `--rate --duration --warmup --replicas --partitions --payload-a/b/c
--window-ms --window-distribution --inference-latency-ms --inference-percent --ml-profile --fault --fault-at
--fault-interval --fault-count --repetitions --repetition-start --seed`. `AGGBENCH_RESULTS=<dir>` writes runs
elsewhere (e.g. for trial runs that should not enter the report). A run takes roughly warm-up + duration + 2–4 minutes.

The higher-fidelity profile (3 brokers, Redis Sentinel) is `./scripts/deploy.sh --ha`; the genuine Redis
failover scenario (`redis-failover`) only runs there.

Live view: `kubectl -n aggbench port-forward svc/grafana 3000` → http://localhost:3000 (anonymous viewer).

## Where to look in the code

| What | Where |
|---|---|
| State machine (shared rules) | `common-domain/.../AggregationLogic.java`, `ActionPlanner.java` |
| Kafka Streams topology / state store | `streams-aggregator/.../AggregationTopology.java`, `AggregationProcessor.java` |
| Streams configuration (A1 vs A2 difference) | `streams-aggregator/.../StreamsConfigFactory.java` |
| Kafka Streams processing guarantee | `exactly_once_v2`, `StreamsConfigFactory` + `ActionIntentPipeline.java` (outbox) |
| Redis atomic merge | `redis-aggregator/src/main/resources/redis/aggregation.lua`, adapter `RedisAggregationStore.java` |
| Event handler (B) | `redis-aggregator/.../FragmentHandler.java` |
| Consumer acknowledgement logic | `action-executor/.../ManagedConsumerLoop.java`, `OffsetTracker.java` |
| Side effects, retries, idempotency | `action-executor/.../ActionExecutor.java`, `persistence-service/.../PersistenceRepository.java` |
| Failure recovery | `RecoveryInstrumentation.java` (A), `RedisHealthMonitor.java` (B), `scripts/chaos.sh` |
| Protocol verification (no silent fallback) | `action-executor/.../ProtocolVerifier.java` |
| Latency measurement | `persistence-service` (commit time), `correctness-verifier/.../PerformanceAnalysis.java` |
| Independent verification | `correctness-verifier/.../CorrectnessCheck.java` |

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) – event flows, ownership, state, recovery, side effects
- [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) – classes, topology, Lua function, acknowledgements
- [docs/BENCHMARK-METHODOLOGY.md](docs/BENCHMARK-METHODOLOGY.md) – boundaries, fairness, scenarios, limitations
- [docs/FAILURE-RECOVERY.md](docs/FAILURE-RECOVERY.md) – what happens on restart, kill, Redis loss
- [docs/RESULTS.md](docs/RESULTS.md) – generated findings (only from executed runs), JVM build;
  [results/native/RESULTS.md](results/native/RESULTS.md) – the same for the native build
- [docs/ANALYSIS-NOTES.md](docs/ANALYSIS-NOTES.md) – manual root-cause notes on specific runs, with evidence
- [docs/DISRUPTION-IMPACT.md](docs/DISRUPTION-IMPACT.md) – late events per rollout and per pod failure, as decision input
- [docs/adr/](docs/adr/) – architecture decision records

## Pinned versions

Kafka brokers and clients 4.2.2 · Quarkus 3.40.1 · Java 25 (Eclipse Temurin 25.0.4.1 JRE images; native
aggregators: Oracle GraalVM 25.0.5, G1) · Redis 7.4.6 ·
PostgreSQL 17.6 · Lettuce 6.8.2 · Prometheus 3.7.2 · Grafana 12.2.1 · Helm charts in `helm/`.
