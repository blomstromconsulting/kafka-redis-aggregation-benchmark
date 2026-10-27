# ADR 0001 – Transactional outbox for side effects in the Kafka Streams variants

**Status:** accepted

**Context.** The stream thread must not block on HTTP calls (ML inference takes 50–300 ms) and Kafka Streams
transactions cannot include external systems. Fire-and-forget calls from the processor would lose actions on crash.

**Decision.** The processor forwards `ActionIntent`s to an intent topic inside the `exactly_once_v2` transaction.
A plain consumer (`read_committed`, `group.protocol=consumer`) in the same pod executes them with the shared
`ActionExecutor` and commits intent offsets only after the persistence service confirms. Effects are idempotent.

**Consequences.** No lost or duplicated durable outcome. Cost: one extra Kafka hop and up to `commit.interval.ms`
(100 ms) before an intent becomes visible; this is part of A's measured end-to-end latency and is shown separately
as "aggregation-only" latency. B needs no outbox because it commits its ingress offset only after its actions are
durable; both give the same business-level guarantee.
