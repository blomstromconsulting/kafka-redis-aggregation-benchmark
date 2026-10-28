# Benchmark methodology

## What is measured, and where

All latencies are computed by the independent verifier from durable rows in PostgreSQL, using **exact
percentiles over every action** (nearest-rank; no histogram buckets, no averaging of percentiles).

| Measurement | Start | End |
|---|---|---|
| 1. Individual fragment latency | broker append time of the fragment | COMMIT of its receipt in PostgreSQL |
| 2. Cumulative (STAGE2) latency | latest broker append time of A and B | COMMIT of the A+B receipt |
| 3. Final aggregate latency | latest broker append time of A, B, C | COMMIT of the persisted aggregate (incl. ML inference) |
| 4. Aggregation-only latency | latest required append time | the executor picks the action up (state merged and, for Streams, the EOS transaction committed and read) |
| 5. First-fragment-to-final (supplementary) | earliest append time of A, B, C | COMMIT of the aggregate (includes the inter-fragment gaps of the workload) |
| 6. Result-ready latency (since 2026-10-27) | as 1–3 | the result exists, before it is persisted: executor pickup (INDIVIDUAL, STAGE2); pickup + ML inference including its queueing (FINAL) |

- *Broker append time*: the ingress topic uses `message.timestamp.type=LogAppendTime`, so every record carries the
  broker's clock at append; the latest/earliest of the relevant fragments is carried through the intents.
- *COMMIT time*: the persistence service records the wall clock right after `COMMIT` returns for each batch
  (`commit_batches.postcommit_us`). Queueing, retries, ML inference, outbox hops, rebalances and restoration
  all fall inside the interval; nothing is excluded.
- *Clocks*: every component runs on the same OrbStack node and therefore the same kernel clock; cross-component
  skew is ≈0 here. In a multi-node production deployment skew between broker and database hosts would add an
  error of the order of the NTP offset (typically < 1 ms with chrony, but must be checked). Local durations inside
  a component use monotonic clocks (`System.nanoTime`).
- Live Prometheus histograms are for observation only; reported numbers never come from them.

The SLO (p99.95 ≤ 500 ms) applies to measurements 1–3 separately; "worst path" is the maximum of the three.
Measurement 6 applies the same rules without the persistence step, whose real-world counterpart is publishing
the result to a Kafka topic and Redis (not modelled): the verifier reports latency, SLO verdict
(`sloResultReady`), recovery and partition stalls on both bases, and reports lead with result ready. In A the
end point already includes the transactional Kafka hop to the executor; in B there is no Kafka publish.

## Workload

`load-generator` is open-loop: aggregate *i* starts at a precomputed time (linear ramp 10 % → 100 % over the
ramp period, then constant), and each fragment is sent at its planned offset regardless of how fast the system
consumes. If the producer falls behind its schedule, the lateness is measured (`max_schedule_lag_ms`); a run whose
achieved rate during measurement is < 98 % of the requested rate, or whose schedule lag exceeds 1 s, is
**INCONCLUSIVE** for the SLO.

Everything about aggregate *i* is a pure function of `(seed, settings, i)` (`WorkloadPlan`): correlation id and
partition (Kafka's murmur2), arrival offsets within the correlation window (uniform/fixed/exponential), order
(in-order or one of the five other permutations with probability *out-of-order %*), duplicates (same event,
sent again up to `duplicateDelayMaxMs` later), stale versions (v0 after v1), missing fragments, inference flag and
payload bytes. With the same seed A1, A2 and B receive the identical logical input. The full manifest is written
to PostgreSQL before sending starts; the verifier only uses the manifest and the durable outputs.

Defaults: payloads 200/200/400 bytes, correlation window 1 s (uniform), 10 % out of order, 2 % duplicates,
1 % missing, 100 % requiring inference, ML 50 ms fixed, 12 partitions, 3 replicas.

**Warm-up** (60 s at target scenarios, including a 30 s ramp) is excluded from statistics: only actions whose
source append time falls inside `[measure_start, measure_end)` count. After the generator stops, the run must
*drain* (all monitored group lags zero for 5 consecutive samples) before verification; otherwise correctness
is INCONCLUSIVE.

## Fairness controls

| Control | How |
|---|---|
| Same replicas, CPU/memory limits, JVM, JVM flags | one `aggregator` Deployment template; `jvmOpts` identical; one Dockerfile |
| Same build of the aggregators | a set of compared runs uses one build for every variant: JVM (`Dockerfile.jvm`, `jvmOpts`) or native (Oracle GraalVM 25, `--gc=G1`, `Dockerfile.native`, `nativeOpts`); recorded per run in `environment.txt` (ADR 0005) |
| Same partitions, payloads, order, distribution, seed | same generator, same seed, same topic settings |
| Same downstream behaviour | same `ActionExecutor` code and settings; persistence and ML simulator redeployed fresh every run |
| Same group membership timing | session timeout 10 s, heartbeat 250 ms (1 s for runs before 2026-10-27, ADR 0003) for classic (client), consumer and streams (group configs) — verified by every pod against the broker at startup |
| Same commit cadence | Streams `commit.interval.ms=100`; consumer loops commit every 100 ms |
| Same poll size | `max.poll.records=500` for every consumer |
| Same back-pressure | consumption pauses at 20,000 not-yet-durable business actions (not Kafka records, because one intent is one action but one fragment can be up to three) |
| Same database state | `CHECKPOINT` before each run; a run's raw rows are exported, deleted and `VACUUM`ed after verification, so maintenance from one run does not fall into the next run's measurement |
| Same retention | completed aggregates 120 s, incomplete 600 s, in both state stores |
| No silent protocol fallback | `ProtocolVerifier` checks group type and group configs; the pod exits on mismatch |
| Isolation | per run: new topics, groups, Redis keyspace (FLUSHALL + check), run id, fresh pods |
| Order effects | runs are sequential; the variant order is rotated per repetition |
| Warm-up / measurement | identical per scenario |

Architectural differences that cannot be equalised and are part of what is being compared:
- A uses `exactly_once_v2` plus a transactional outbox and an extra Kafka hop; B uses idempotent at-least-once
  without an outbox. Both give the same business guarantee (no lost action, no duplicate durable outcome).
- A keeps state in-process (RocksDB) with a standby copy and changelog writes on the broker; B keeps it in a
  separate Redis server. Resource figures therefore list Kafka and Redis separately.
- A1's broker-side assignor has no warm-up tasks in Kafka 4.2; A2's client-side assignor does.

Settings that are deliberately *not* tuned per variant: RocksDB beyond the bounded-memory pattern, Redis beyond
AOF/noeviction, broker defaults other than the group settings above.

## Scenarios (`scenarios/*.env`)

| Id | File | Load | Fault |
|---|---|---|---|
| — | `smoke`, `medium`, `stress` | 100 / 1,000 / 10,000 fragments/s | none |
| S1 | `steady` | 6,000/s, 60 s warm-up, 300 s | none |
| S2 | `rolling` | 6,000/s | `kubectl rollout restart` at +60 s |
| S3 | `abrupt-pod`, `abrupt-container` | 6,000/s | SIGKILL: new pod / same pod |
| S4 | `cold-pod` | 6,000/s | graceful delete, replacement starts with empty state |
| S5 | `rescheduling` | 6,000/s, 330 s | 6 graceful pod replacements, every 45 s |
| S6 | `state-1s/10s/60s/300s` | 6,000/s, warm-up ≥ window + 60 s | cold replacement; correlation window 1/10/60/300 s |
| S7 | `slow-ml` | 6,000/s | inference 50 → 300 ms for 60 s |
| S8 | `redis-restart`, `redis-loss`, `redis-failover` (HA) | 6,000/s, B only | Redis restart / crash losing 5 s / Sentinel failover |
| S9 | `correctness` | 1,000/s | 10 % dup, 50 % reordered, 5 % stale, 5 % missing, 1 % downstream errors (also after commit), pod kill |
| S10 | `kafka-restart` (optional) | 1,000/s | broker restart (full outage in the local profile) |

Suites: `smoke`, `core` (S1–S5), `full`, `extended` (stress, 300 s window, Kafka restart, Redis failover).
Default repetitions: 3 (configurable, e.g. `--repetitions 5`).

## Windows and recovery metrics

- **Steady window**: measurement window minus failure windows. **Failure window**: from each `FAULT_*` event
  (recorded by `chaos.sh` immediately before acting, ±0.3 s) for 60 s or until the next fault.
  An action belongs to a window by the broker append time of the fragment it depends on, so an action appended
  shortly before a fault and delayed by it counts as steady. `scripts/analysis/window_sensitivity.py` recomputes
  the split from `latencies.csv.gz` with an overlap rule (action in flight at any time during the window); the
  headline S2–S5 figures use the whole measurement window and do not depend on either rule.
- **Partition stall**: per ingress partition, the largest gap between consecutive durable completions after the
  fault; stalls > 2 s are listed with the time at which processing resumed.
- **Latency recovery**: first second after the fault from which per-second p99 stays ≤ 500 ms for 10 s.
- **Late events per disruption**: incoming events whose own action exceeded 500 ms among actions in flight during
  the 120 s after a fault, minus the run's own rate outside that period; also events over 2 s and 10 s. Saved per
  run as `disruption.json` and tabulated per variant in the report (method: `docs/DISRUPTION-IMPACT.md`).
- **Backlog recovery**: time until the committed-offset lag returns to ≤ max(2 × pre-fault p95, 100) for 5 samples.
- Affected keys: FINAL aggregates over the SLO inside the window, with partition and latency (sample of 20).
- Restoration / assignment / standby / replay timelines come from application events (`bench_events`).

## SLO classification

Per run: `loadAchieved`, correctness (PASS / FAIL / INCONCLUSIVE), steady-window latency and failure-window latency
(PASS if p99.95 ≤ 500 ms on all three paths; INCONCLUSIVE if any path has < 2,000 samples, because a p99.95 from
fewer samples rests on a single observation). Overall PASS only if all hold. Correctness FAIL invalidates the
performance result. The report additionally marks a run INCONCLUSIVE when the node CPU p95 exceeded 90 %
(resource-saturated laptop). Repetitions are reported individually and as median [min–max].

## Limitations

- One node: no network partitions, no node loss, shared disk and CPU; rescheduling means "new pod on the same
  node". The HA profile adds broker/Redis redundancy but not independent failure domains.
- The laptop is shared with other workloads (other namespaces on the same cluster). Their load adds noise;
  saturation is detected and flagged, not prevented.
- Broker-side metrics come from the clients and the lag monitor (no broker JMX exporter).
- GC pauses come from GC logs of the aggregator pods (JVM `-Xlog:gc`, native `-XX:+PrintGC`, same format),
  saved per run as `gc.json`. Only pods alive at the end of a run are collected, so in fault scenarios the
  replaced pods' pauses are missing.
- Container CPU and memory come from the kubelet resource endpoint. Its samples carry explicit timestamps, so
  Prometheus writes no staleness marker and returns a deleted pod's last value for five more minutes;
  `prom_export.py` keeps only samples younger than 45 s, and group figures are statistics of the instantaneous
  group total (a replaced pod and its successor are never added up). Working-set memory includes active page
  cache, so a broker or database near its limit is not necessarily short of heap.
- The ML service is a simulator (latency/queueing/errors), not a model.
- The shared PostgreSQL (one pod, VM disk, `synchronous_commit=on`, ~10,000 durable writes/s at target load)
  occasionally stalls for 1–2 s (WAL/checkpoint/vacuum I/O). Such stalls hit every variant and are included in
  the end-to-end figures; each run saves the persistence batch latency and queue depth series
  (`resources.json`) so they can be told apart from aggregation stalls, and the aggregation-only latency is
  reported separately.
