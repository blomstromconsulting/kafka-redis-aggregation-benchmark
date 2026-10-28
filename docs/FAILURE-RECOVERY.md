# Failure and recovery behaviour

This document states what each variant does in each failure case and what the benchmark measures.
Measured outcomes are in `docs/RESULTS.md`; nothing here is a measurement.

## Kinds of application failure (and how `scripts/chaos.sh` produces them)

| Fault | Mechanism | Group membership | Local state (`emptyDir`) |
|---|---|---|---|
| Graceful restart / rolling upgrade (`rolling`, `delete-pod`, `reschedule`) | SIGTERM, up to 45 s grace | member leaves the group on close (immediate rebalance) | **lost** (new pod) |
| Abrupt pod failure (`kill-pod`) | SIGKILL of the JVM container, then force delete of the pod | member silently disappears; detected after the 10 s session timeout | **lost** (new pod) |
| Process crash (`kill-container`) | `docker kill -s KILL` of the container | same as above, but the restarted container rejoins quickly | **kept** (same pod, same `emptyDir`) |

A container restart is not a pod replacement: only `kill-container` keeps the RocksDB directory.
Note: `kubectl delete pod --grace-period=0 --force` on its own is **not** an abrupt failure — the kubelet still
sends SIGTERM and the JVM shuts down gracefully (observed in validation: the pod left its group within 80 ms).
`chaos.sh` therefore SIGKILLs the container through the Docker engine first. Scenarios S3
(abrupt) therefore exist in both forms, and S4 (cold replacement) uses a new pod.

## A1 / A2 — Kafka Streams

**Graceful shutdown.** `KafkaStreams.close(LEAVE_GROUP)`: in-flight work of the current transaction is committed,
the member leaves, its active tasks are reassigned. The intent consumer then drains (≤ 20 s) and commits.

**Task migration.** A task moves to another pod. If that pod holds the task's standby replica (and the standby is
caught up — measured as standby lag), the task becomes active after restoring only the small standby lag. If not,
the task must restore its stores from the changelog topics; processing for its partition is paused until then.
- A2 (classic, HighAvailabilityTaskAssignor) keeps an active task on a caught-up client and may assign
  *warm-up* replicas to the new pod, moving the active task only after a probing rebalance finds it caught up
  (`acceptable.recovery.lag=10000`, `probing.rebalance.interval.ms=600000`, `max.warmup.replicas=2`).
- A1 (streams protocol, Kafka 4.2) uses the broker-side **sticky assignor only; warm-up tasks are not
  supported**. A new pod can receive active tasks that it has to restore cold.

**Abrupt failure.** Nothing happens until the broker detects the missing heartbeats (session timeout 10 s); then the
dead member's tasks are reassigned (standby → active where available). The aborted transaction of the dead
instance is fenced; records since its last commit are reprocessed by the new owner exactly once.

**Restoration** reads the changelog from the local checkpoint (or from the beginning for a new pod) up to the end
offset. Its cost grows with the number of changelog records, not only with live state: compaction lags behind
writes, so recently updated keys appear several times. Instrumented via `RESTORE_START/RESTORE_END` events
(records, duration), `bench_streams_restoring_partitions`, `bench_streams_restored_records_total`.

**Intents in flight.** Intent offsets are committed only after the action is durable; if the intent consumer
dies, another member re-reads the intents and the persistence service answers DUPLICATE for anything already done.

## B — Kafka consumers + Redis

**Graceful shutdown.** Stop polling, wait (≤ 20 s) until every in-flight record's actions are durable, commit,
close the consumer (leave group). Revoked partitions are drained and committed in `onPartitionsRevoked`.

**Abrupt failure.** Partitions of the dead member are reassigned after the session timeout; the new owner resumes
from the last committed offset. Records processed but not committed are delivered again: their merges are
recognised as `REDELIVERED` (same partition:offset) and re-issue exactly the actions they triggered; the
persistence service deduplicates. No state needs restoring: the new owner reads Redis.

**The five cases required by the task**

1. *Redis accepted a fragment, consumer crashed before the offset commit* — the record is redelivered, the merge
   returns `REDELIVERED` with the same triggered stages, actions are re-executed idempotently. No loss, no
   duplicate outcome.
2. *Offset committed, then Redis lost acknowledged state* — possible: AOF `everysec` can lose ≈1 s on a crash,
   and Sentinel failover can lose whatever was not yet replicated. Mitigation: every pod polls the Redis
   `run_id` every 500 ms; a change (restart or failover) makes it re-read its partitions from
   `now − replayWindow` (default 60 s). Idempotent merges rebuild the lost fragments; INDIVIDUAL actions of
   re-read records that had completed before are skipped, STAGE2/FINAL actions are executed again because the
   rebuilt state can complete an aggregate that the damaged state could not (persistence absorbs repeats).
   **Limitation:** an aggregate whose fragments are further apart than `replayWindow` cannot be rebuilt; it stays
   incomplete and the verifier reports it as a missing final aggregate. `maxmemory-policy noeviction` prevents
   silent eviction; a full Redis rejects writes (retried) instead of losing state.
3. *Redis reports a complete aggregate but persistence fails* — the action is retried with backoff until the
   persistence service confirms it; the record's offset is not committed meanwhile. If the pod dies, case 1
   applies.
4. *Persistence succeeded but the offset commit failed* — the record is redelivered later; the persistence
   service answers DUPLICATE (same key, same checksum). The benchmark injects this deliberately
   (`errorAfterCommitRate`, scenario `correctness`).
5. *Consumer restarts while asynchronous actions are in flight* — those records were not committed (commit
   position = first unfinished record), so they are redelivered and re-executed idempotently.

**Correctness vs latency during Redis failover.** Correctness relies only on Kafka retention + idempotent merges and
actions. Latency suffers twice: merges stall while Redis is unreachable (retries), and the replay re-reads up to
`replayWindow` × input rate records. The benchmark distinguishes:
- `redis-restart` — graceful restart, AOF intact (no acknowledged loss; tests reconnection + replay cost),
- `redis-loss` — crash that loses the last ~5 s of acknowledged writes (AOF disabled 5 s before
  `SHUTDOWN NOSAVE`, so the restarted server loads stale data): models asynchronous-replication loss. Each run
  starts with a fresh Redis pod, because the kubelet delays a container that exits again within 10 minutes by
  10 s or more (crash back-off), which would otherwise add to the run after an earlier redis-loss run,
- `redis-failover` — genuine Sentinel promotion of the replica (HA profile only).

## Incomplete aggregates and pending finalization

- Incomplete aggregates are kept for `incompleteTtl` (600 s) in both variants, then expired and counted
  (`bench_incomplete_expired_total`, `INCOMPLETE_EXPIRED` events). Gauges: `bench_incomplete_aggregates`
  (A: open-index entries per task; B: size of the open set).
- Pending finalization (FINAL triggered, aggregate not yet persisted): `bench_pending_finalization`. It is
  recoverable because the triggering record (B) or intent (A) is not committed until persistence confirms.
- The verifier identifies every incomplete input sequence from the manifest and checks that no aggregate was
  persisted for it, and that its individual (and, if A and B arrived, STAGE2) actions exist.

## Kafka infrastructure failure (S10, optional)

`kafka-restart` deletes the broker pod. In the local profile this is a full Kafka outage: producers, Streams and
consumers block and retry; the load generator's schedule lag shows the outage. This is a different class of
failure from application failure (all variants stop equally) and is reported separately.
