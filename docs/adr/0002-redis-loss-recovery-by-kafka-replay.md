# ADR 0002 – Redis state loss is recovered by a bounded Kafka replay

**Status:** accepted

**Context.** Redis acknowledges writes before they are durable (AOF `everysec`) or replicated (async replication),
so a crash or failover can lose acknowledged aggregation state after the Kafka offset was committed. A durable
fragment inbox would add a database write per fragment.

**Decision.** Kafka is the source of truth. Pods poll the Redis `run_id`; when it changes they seek all assigned
partitions back to `now − replayWindow` (default 60 s, per scenario ≥ correlation window + 60 s) and re-merge.
Merges are idempotent (`REDELIVERED`/`DUPLICATE`), INDIVIDUAL actions of re-read, already-completed records are
skipped, stage actions are re-executed and deduplicated by the persistence service.

**Consequences.** Correctness after loss is bounded by `replayWindow` (documented limitation) and Kafka retention.
Latency after a Redis restart includes the replay of up to `replayWindow × rate` records. `noeviction` prevents
silent state loss under memory pressure.
