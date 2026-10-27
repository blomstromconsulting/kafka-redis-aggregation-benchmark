# Implementation

Maven multi-module project, Java 21, Quarkus 3.40.1 (JVM mode), Kafka clients/Streams 4.2.2 used directly
(no Quarkus Kafka extension, so every Kafka setting is visible in code).

| Module | Kind | Responsibility |
|---|---|---|
| `common-domain` | library | Event model, the aggregation state machine, action planning, hashes, inference formula |
| `action-executor` | library | Shared by A and B: durable action execution, consumer loop with commit-after-completion, protocol verifier, timeline events |
| `streams-aggregator` | Quarkus app | A1 and A2 (same image) |
| `redis-aggregator` | Quarkus app | B |
| `persistence-service` | Quarkus app | Idempotent receipts/aggregates in PostgreSQL, commit timestamps, fault injection |
| `ml-simulator` | Quarkus app | KServe Open Inference Protocol v2 simulator with latency/concurrency/error profiles |
| `load-generator` | Quarkus app (Job) | Deterministic open-loop workload, manifest, producer and lag sampling |
| `correctness-verifier` | CLI (fat jar) | Independent correctness checks and latency/recovery analysis from durable outputs |

## Shared domain (`common-domain`)

- `FragmentEvent` – ingress value: `runId, correlationId, eventId (<corr>:<type>:v<version>), eventType, version,
  requiresInference, payload, generatedAtUs`. Key = `correlationId`.
- `AggregationState` – immutable state per correlation id: up to three `StoredFragment`s (event id, version,
  payload, payload hash, append time, `recordRef = partition:offset`, stages that this fragment triggered),
  superseded versions, `stage2Emitted`, `finalEmitted`, timestamps and counters.
- `AggregationLogic.merge(state, event, position, hash, now)` – the state machine. Evaluation order:
  1. no fragment of that type → `ACCEPTED`;
  2. same event id (stored or superseded) → `REDELIVERED` if it is the same Kafka record, else `DUPLICATE`;
  3. version ≤ stored → `STALE`;
  4. newer version but an emitted stage already used that type → `LATE_VERSION` (aggregate frozen);
  5. otherwise → `SUPERSEDED`.
  After ACCEPTED/SUPERSEDED, STAGE2 is triggered once when A and B are present and FINAL once when A, B and C
  are present. The triggered stages are stored on the fragment so that a crash redelivery re-issues them.
- `ActionPlanner.plan(...)` – turns a merge result into `ActionIntent`s: INDIVIDUAL (unless DUPLICATE/STALE),
  STAGE2, FINAL; computes content hashes and the source timestamps used for latency.
- `Hashing` – SHA-256 of payloads; A+B hash and aggregate checksum formulas (the verifier recomputes them).
- `InferenceModel` – deterministic score from the aggregate checksum (simulator computes it, verifier checks it).
- Test fixtures `ArrivalScenarios` (test-jar): all six orders, duplicates, crash redelivery, stale, superseded,
  late versions, missing fragments – each delivery with its expected outcome and stages. Run against the Java
  logic, the Streams topology (`TopologyTestDriver`) and the Lua function (Testcontainers Redis).

## A1 / A2: `streams-aggregator`

- `AggregationTopology` – Processor API topology: source `fragments` → `AggregationProcessor` → sink
  `action-intents`; stores `aggregation-store` (`AggregationState` JSON) and `expiry-index`, both persistent
  (RocksDB), caching enabled, changelogged with `segment.bytes=64 MiB`, `min.cleanable.dirty.ratio=0.3`.
- `AggregationProcessor` – per record: `store.get`, `AggregationLogic.merge`, `store.put`, index update,
  `context.forward` of each intent. A wall-clock punctuator (5 s) range-scans the expiry index:
  `C|<completedMs>|key` evicts completed aggregates after `completedRetention` (120 s);
  `O|<createdMs>|key` expires incomplete aggregates after `incompleteTtl` (600 s, counted). KTable caching is not
  used for forwarding, so no action is suppressed.
- `StreamsConfigFactory` – the only place where A1 and A2 differ: `group.protocol=streams|classic`, and for A2 the
  client-side `num.standby.replicas`, `session.timeout.ms`, `heartbeat.interval.ms`. Both: `exactly_once_v2`,
  `commit.interval.ms=100`, one stream thread, `max.poll.records=500`, producer `linger.ms=5`,
  `BoundedRocksDbConfig` (shared 128 MiB block cache + 64 MiB write-buffer manager per pod).
- `ActionIntentPipeline` – intent consumer (`ManagedConsumerLoop`, `read_committed`, `group.protocol=consumer`)
  feeding the `ActionExecutor`.
- `RecoveryInstrumentation` – `StateRestoreListener` (restore start/end, records, duration),
  `StandbyUpdateListener` (standby lag = changelog end − standby position), state listener, 1 s task-metadata
  poll (active/standby task sets, `TASKS_CHANGED` events). `AggregationProcessor` emits
  `TASK_PROCESSING_STARTED` with the age of the first record processed after a task (re)starts.
- `StreamsRuntime` – lifecycle; graceful shutdown closes Kafka Streams with
  `CloseOptions.groupMembershipOperation(LEAVE_GROUP)` (dynamic members would otherwise wait for the session
  timeout), then drains the intent pipeline.

## B: `redis-aggregator`

- `redis/aggregation.lua` – Redis Function library `aggbench`:
  - `aggbench_merge(KEYS: hash, openSet; ARGV: run, corr, type, eventId, version, payload, hash, inference,
    appendMs, ref, now, incompleteTtl, completedRetention)` – the same rules as `AggregationLogic`; returns
    `{outcome, stagesDue[, HGETALL]}` (the hash is returned only when stages are due, to build the intents).
    Sets `PEXPIRE` incompleteTtl on creation and completedRetention on completion; maintains the open set.
  - `aggbench_sweep_expired(openSet; cutoff, limit)` – removes expired entries and returns how many *this call*
    removed (several pods may sweep without double counting).
- `RedisAggregationStore` – the Redis adapter (Lettuce, one connection, async/pipelined `FCALL`), loads the
  function library at startup, parses replies into `AggregationState`, reads `INFO` (run_id, memory).
- `FragmentHandler` – per poll: for each record compute the payload hash, `merge` (retried with backoff until it
  succeeds; merges are idempotent), plan intents with `ActionPlanner`, execute with `ActionExecutor`; the record's
  future completes when every triggered action is durable.
- `RedisHealthMonitor` – Redis loss detection (run_id change → replay request) and sampling of open aggregates,
  Redis memory, expired incomplete aggregates.
- `RedisRuntime` – lifecycle; consumer `group.protocol=consumer`, manual commits, client id = pod name.

## Shared runtime: `action-executor`

- `ManagedConsumerLoop` – the acknowledgement logic used by B's ingress consumer and A's intent consumer:
  register offset → handler returns one future per record → `OffsetTracker` → `commitAsync` every 100 ms of the
  first unfinished offset per partition (for idle partitions the fetch position, so transaction markers do not
  leave a lag of one). Back-pressure: pauses all partitions when 20,000 business actions are not yet durable
  (A: intents in the executor; B: actions in the executor plus pending Redis merges) and resumes below 80 %. On revoke: waits up to 15 s for the
  revoked partitions' work, commits synchronously. On loss: drops without commit. Replay requests (B) seek back
  by timestamp and mark re-read records that had already completed so that their INDIVIDUAL action is skipped.
- `ActionExecutor` – INDIVIDUAL/STAGE2 → `POST /api/receipts`; FINAL → optional `POST /v2/models/aggregate-scorer/infer`
  (bounded by `maxInFlightInference`) → `POST /api/aggregates`. `Batcher` sends natural batches (no linger) with
  4 concurrent requests per endpoint; failures are retried with exponential backoff (20 ms → 1 s) until success
  or shutdown. CREATED and DUPLICATE complete the action; CONFLICT completes it and is counted (the verifier fails
  the run).
- `ProtocolVerifier` – after startup asks the broker for each group's actual type (STREAMS / CLASSIC / CONSUMER)
  and group-level configs (standby replicas, session timeout, heartbeat) and exits the application on any
  mismatch. The report is served at `GET /status` and saved per run (`startup-status-*.json`).
- `BenchEvents` – timeline events, logged and shipped to `persistence-service` (`bench_events`).

## Persistence (`persistence-service`)

`POST /api/receipts` and `POST /api/aggregates` write one batch per transaction with
`INSERT … SELECT FROM unnest(...) ON CONFLICT DO UPDATE SET duplicate_attempts = +1 RETURNING key, (xmax = 0), hash`.
`inserted` distinguishes CREATED from DUPLICATE; a different hash/version for an existing key is a CONFLICT,
recorded in `conflicts`, never overwriting the row. After `COMMIT` returns, the batch's commit time is stored in
`commit_batches` (asynchronous commit; it is measurement data). Final aggregates are unique per
`(run_id, correlation_id)` and carry version 1, checksum, all three payloads and the ML score.
Configurable latency and two error modes (before write, after commit) via env or `POST /admin/faults`.

## Deduplication summary

| Situation | A1/A2 | B |
|---|---|---|
| Same Kafka record re-read after crash | impossible to observe (EOS aborts the transaction) | `REDELIVERED`, same actions re-issued |
| Logical duplicate (new record, same event id) | `DUPLICATE`, no actions | `DUPLICATE`, no actions |
| Action retried after unknown outcome | persistence `DUPLICATE` | persistence `DUPLICATE` |
| Intent re-read after intent-consumer crash | persistence `DUPLICATE` | n/a |

## Configuration

Every runtime value comes from Helm (`helm/benchmark/values.yaml`) via environment variables; defaults in each
module's `application.properties`. Secrets: the PostgreSQL password is generated by the infrastructure chart into
Secret `aggbench-postgres` and injected from it; nothing is hard-coded.
