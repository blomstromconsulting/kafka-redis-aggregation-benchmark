# Architecture

## Business flow (identical for every variant)

Three fragments share a `correlationId`; each carries disjoint information. Every fragment is actionable on its own:

| Arrival | Always | When the condition first holds |
|---|---|---|
| fragment A, B or C | **INDIVIDUAL** action for that fragment (durable receipt `I:<eventId>`) | — |
| A and B present | — | **STAGE2**: build the cumulative A+B representation, execute its action (receipt `S2:<corr>`) |
| A, B and C present | — | **FINAL**: build A+B+C, run ML inference (KServe v2), persist the aggregate (`F:<corr>`) |

The life of one aggregate. Every fragment except a duplicate or a stale version makes its own INDIVIDUAL
action due; the transitions show the actions that become due in addition:

```mermaid
stateDiagram-v2
    direction LR
    [*] --> PARTIAL: first fragment
    PARTIAL --> CUMULATIVE_AB: A and B present, C not yet<br/>+ STAGE2
    PARTIAL --> COMPLETE: A or B arrives last<br/>+ STAGE2 + FINAL
    CUMULATIVE_AB --> COMPLETE: C arrives<br/>+ FINAL
    COMPLETE --> [*]: evicted 120 s after FINAL<br/>(completedRetention)
    PARTIAL --> EXPIRED
    CUMULATIVE_AB --> EXPIRED
    EXPIRED --> [*]
    EXPIRED: EXPIRED
    EXPIRED: no FINAL 600 s after<br/>the first fragment (incompleteTtl)
```

Fragments may arrive in any order (with the last one, STAGE2 and FINAL can become due together), may be
duplicated, may come in stale versions, and may be missing (the aggregate stays incomplete and observable,
and when it expires it is counted, `INCOMPLETE_EXPIRED`, never persisted). A duplicate or stale delivery
changes nothing; a redelivered Kafka record re-issues exactly the actions it triggered the first time.
The rules are one pure state machine (`AggregationLogic`) implemented once in Java and once in Lua, tested
against the same fixtures.

## System overview

Blue is the system under test, the only part that differs between variants. Green is the measurement
environment, identical for every variant; amber is used by variant B only; grey is shared infrastructure and
the scripts on the host. The same colours are used in every diagram below. Prometheus and Grafana (not drawn)
watch every pod; no reported latency comes from them.

```mermaid
flowchart TB
    scripts["benchmark.sh · chaos.sh<br/>on the host"]
    lg["load-generator · Job<br/>open loop, seeded<br/>+ consumer-lag monitor"]
    kafka[["Kafka 4.2.2<br/>ingress topic, 12 partitions<br/>key = correlationId<br/>LogAppendTime"]]
    agg["system under test<br/>Deployment, 3 pods<br/>A1 / A2: Kafka Streams<br/>B: Kafka consumers<br/>+ shared ActionExecutor"]
    redis[("Redis 7.4<br/>B only")]
    ml["ml-simulator<br/>KServe v2 /infer"]
    ps["persistence-service<br/>idempotent receipts<br/>and aggregates"]
    pg[("PostgreSQL 17<br/>manifest, receipts,<br/>aggregates")]
    verifier["correctness-verifier<br/>on the host<br/>manifest vs durable outputs"]

    lg -- "fragments A, B, C" --> kafka
    kafka --> agg
    agg <-- "merge" --> redis
    agg -- "FINAL: inference" --> ml
    agg -- "every action" --> ps
    ps --> pg
    lg -- "manifest (ground truth)" --> pg
    pg --> verifier
    scripts -. "rollouts, kills" .-> agg

    classDef sut fill:#dbeafe,stroke:#1d4ed8,stroke-width:3px,color:#0b1b3a
    classDef meas fill:#dcfce7,stroke:#15803d,color:#052e16
    classDef bonly fill:#fef3c7,stroke:#b45309,color:#3b2105
    classDef infra fill:#e2e8f0,stroke:#475569,color:#0f172a
    class agg sut
    class lg,ml,ps,verifier meas
    class redis bonly
    class kafka,pg,scripts infra
```

## A1 / A2 — Kafka Streams

Both halves run in every aggregator pod; the dashed box is what one transaction commits, together with the
ingress offsets:

```mermaid
flowchart LR
    ingress[["ingress<br/>p0 … p11"]]
    proc["stream thread<br/>AggregationProcessor<br/>one task per partition"]
    store[("RocksDB on emptyDir<br/>aggregation-store<br/>expiry-index")]
    subgraph tx["one Kafka transaction<br/>every 100 ms"]
        direction TB
        intents[["action-intent<br/>p0 … p11 · outbox"]]
        cl[["changelog topics<br/>compacted"]]
    end
    ic["intent consumer<br/>read_committed<br/>+ ActionExecutor"]
    ml["ml-simulator"]
    ps["persistence-service"]

    ingress --> proc
    proc <--> store
    proc --> intents
    proc --> cl
    intents -- "after the commit" --> ic
    ic -- "FINAL" --> ml
    ic --> ps
    ic -. "commit offset<br/>once durable" .-> intents

    classDef sut fill:#dbeafe,stroke:#1d4ed8,color:#0b1b3a
    classDef meas fill:#dcfce7,stroke:#15803d,color:#052e16
    classDef infra fill:#e2e8f0,stroke:#475569,color:#0f172a
    class proc,store,ic sut
    class ml,ps meas
    class ingress,cl,intents infra
    style tx fill:transparent,stroke:#475569,stroke-dasharray:6 4
```

One fragment's path, with the instants the verifier measures latency between:

```mermaid
sequenceDiagram
    participant LG as load-generator
    participant K as Kafka
    participant ST as stream thread
    participant IC as intent consumer<br/>+ ActionExecutor
    participant ML as ml-simulator
    participant PS as persistence-service<br/>+ PostgreSQL

    LG->>K: fragment to ingress
    Note over K: T0 broker append time
    K->>ST: poll ingress
    rect rgba(37, 99, 235, 0.12)
        ST->>ST: read state (RocksDB), merge, write state
        ST->>K: changelog records + action intents
        Note over ST,K: one transaction, committed every 100 ms<br/>with the ingress offsets
    end
    K->>IC: poll action-intent (read_committed)
    Note over IC: T1 executor picks the action up
    opt FINAL that requires inference
        IC->>ML: POST /v2/models/…/infer
        ML-->>IC: score
        Note over IC: T2 inference done
    end
    IC->>PS: POST /api/receipts or /api/aggregates (batched)
    Note over PS: T3 COMMIT returned
    PS-->>IC: CREATED or DUPLICATE
    IC->>K: commit intent offsets up to the first unfinished intent
```

T0 is the broker append time of the fragment (for STAGE2 and FINAL, the latest append time of the required
fragments). Aggregation-only latency is T0→T1; result ready is T0→T1, or T0→T2 for FINAL; end to end
is T0→T3 (`docs/BENCHMARK-METHODOLOGY.md`). In A, T1 already includes the wait for the transaction commit and
the extra Kafka hop.

- **Ownership**: one task per ingress partition. A task (and its state) is owned by exactly one active pod;
  with three pods and 12 partitions each pod owns ~4 active tasks and ~4 standby tasks.
- **State**: per correlation id an `AggregationState` (fragments, versions, record refs, stage flags) in RocksDB on
  the pod's `emptyDir`. Every write is also in the changelog topic, which is the durable copy.
- **Processing guarantee**: `exactly_once_v2`. State changelog writes, the forwarded action intents and the input
  offset commit are one Kafka transaction (every `commit.interval.ms` = 100 ms).
- **External side effects**: never called from the stream thread. Intents go to a transactional outbox topic;
  a separate consumer (same pod, `group.protocol=consumer`, `read_committed`) executes them and commits the
  intent offsets only after the action is durable. Cost: one extra Kafka hop + waiting for the EOS commit.
- **Standby replicas**: one per task. A1 configures them as a *group* config (`streams.num.standby.replicas=1`,
  set by `benchmark.sh`, verified by every pod); A2 as a *client* config (`num.standby.replicas=1`).
- **A1 vs A2**: identical image and topology. A1 uses the broker-side Streams rebalance protocol (KIP-1071; in 4.2
  only the sticky assignor, no warm-up tasks). A2 uses the classic protocol with the client-side
  HighAvailabilityTaskAssignor (warm-up tasks, probing rebalances every 10 min by default).
- **Recovery**: a task moving to a pod that already hosts its standby resumes almost immediately; a task moving to
  a pod without local state must restore its stores from the changelog before processing resumes.

Where a task can resume, shown for task `0_3` (red: a pod without the task's state):

```mermaid
flowchart LR
    in3[["ingress p3"]] --> act
    subgraph p1["pod 1"]
        act["task 0_3 · active"]
    end
    act -- "every state write" --> cl3[["changelog p3"]]
    cl3 -- "applied continuously" --> sb
    subgraph p2["pod 2"]
        sb["task 0_3 · standby<br/>state nearly current"]
    end
    subgraph p4["new pod · empty emptyDir"]
        cold["task 0_3 · no state"]
    end
    cl3 -. "whole partition restored<br/>before p3 is processed again" .-> cold

    classDef sut fill:#dbeafe,stroke:#1d4ed8,color:#0b1b3a
    classDef cold fill:#fee2e2,stroke:#b91c1c,color:#450a0a
    classDef infra fill:#e2e8f0,stroke:#475569,color:#0f172a
    class act,sb sut
    class cold cold
    class in3,cl3 infra
```

If pod 1 goes, the cheap move is to pod 2, which only has to apply the last few changelog records. The two group
protocols differ in how often a task lands on a pod without state:

```mermaid
flowchart TB
    join["a pod joins with an empty emptyDir<br/>(rollout, reschedule, replacement)"]
    join --> A1 & A2
    subgraph A1["A1 · streams protocol"]
        direction TB
        a1["broker-side sticky assignor<br/>may give it active tasks<br/>at once"] --> a1r["it restores them<br/>from the changelog;<br/>their partitions wait"]
    end
    subgraph A2["A2 · classic protocol"]
        direction TB
        a2["HighAvailabilityTaskAssignor<br/>keeps active tasks<br/>on caught-up pods;<br/>the new pod gets warm-ups"] --> a2r["a probing rebalance<br/>(every 10 min) moves<br/>an active task once its<br/>warm-up has caught up"]
    end

    classDef sut fill:#dbeafe,stroke:#1d4ed8,color:#0b1b3a
    classDef cold fill:#fee2e2,stroke:#b91c1c,color:#450a0a
    class a1,a2,a2r sut
    class a1r cold
```

## B — Kafka consumers + Redis

Every aggregator pod is stateless; the state lives in Redis:

```mermaid
flowchart LR
    ingress[["ingress<br/>p0 … p11"]]
    c["consumer<br/>group.protocol=consumer"]
    subgraph redis["Redis 7.4"]
        direction TB
        fn["Function aggbench_merge<br/>atomic, idempotent"]
        hash[("hash ab:#lt;run#gt;:#lt;corr#gt;<br/>per correlation id")]
        open[("sorted set ab:#lt;run#gt;:open<br/>incomplete aggregates")]
        fn --> hash
        fn --> open
    end
    ex["ActionExecutor"]
    ml["ml-simulator"]
    ps["persistence-service"]
    hm["RedisHealthMonitor<br/>reads run_id every 500 ms"]

    ingress --> c
    c -- "FCALL, pipelined" --> fn
    fn -- "actions due" --> c
    c --> ex
    ex -- "FINAL" --> ml
    ex --> ps
    ex -. "commit offset<br/>once durable" .-> ingress
    hm -. "run_id changed:<br/>replay from now − 60 s" .-> c

    classDef sut fill:#dbeafe,stroke:#1d4ed8,color:#0b1b3a
    classDef meas fill:#dcfce7,stroke:#15803d,color:#052e16
    classDef bonly fill:#fef3c7,stroke:#b45309,color:#3b2105
    classDef infra fill:#e2e8f0,stroke:#475569,color:#0f172a
    class c,ex,hm sut
    class fn,hash,open bonly
    class ml,ps meas
    class ingress infra
    style redis fill:transparent,stroke:#b45309,stroke-width:2px
```

One fragment's path, with the same measurement instants as in A:

```mermaid
sequenceDiagram
    participant LG as load-generator
    participant K as Kafka
    participant C as consumer<br/>+ ActionExecutor
    participant R as Redis
    participant ML as ml-simulator
    participant PS as persistence-service<br/>+ PostgreSQL

    LG->>K: fragment to ingress
    Note over K: T0 broker append time
    K->>C: poll ingress
    C->>R: FCALL aggbench_merge
    R-->>C: outcome + actions due (one atomic step)
    Note over C: T1 executor picks the action up
    opt FINAL that requires inference
        C->>ML: POST /v2/models/…/infer
        ML-->>C: score
        Note over C: T2 inference done
    end
    C->>PS: POST /api/receipts or /api/aggregates (batched)
    Note over PS: T3 COMMIT returned
    PS-->>C: CREATED or DUPLICATE
    C->>K: commit offsets up to the first record with an unfinished action
```

- **Ownership**: partitions are assigned by the broker (KIP-848, incremental). Pods are stateless; any pod can
  process any partition immediately after assignment because the state is in Redis.
- **State**: one Redis hash per correlation id, modified only by the `aggbench_merge` Redis Function (atomic,
  idempotent). Commands from a consumer are pipelined on one connection, so per-key order is the partition order.
- **Processing guarantee**: at-least-once with idempotent effects. Redelivery of the *same Kafka record*
  (same partition:offset) is recognised (`REDELIVERED`) and re-issues exactly the actions that record triggered.
- **External side effects**: executed directly by the consumer pod with the shared ActionExecutor; no outbox is
  needed because the offset is only committed after every action of the record is durable.
- **Redis durability**: AOF `appendfsync everysec` (local profile) or async replication + Sentinel (HA profile).
  Neither makes acknowledged writes strongly durable. Kafka is the source of truth: when a pod sees the Redis
  `run_id` change (restart or failover), it re-reads its partitions from `now − replayWindow` (default 60 s) and
  rebuilds lost state; see FAILURE-RECOVERY.md.

## Equivalent business guarantees

| Guarantee | A1/A2 | B |
|---|---|---|
| Fragment state change + "which actions are due" | atomic (EOS transaction) | atomic (Redis Function) |
| Action durably executed at least once | outbox + commit-after-completion | commit-after-completion |
| No duplicate durable business outcome | idempotency keys in PostgreSQL | idempotency keys in PostgreSQL |
| State survives pod loss | changelog (Kafka, durable) | Redis (not strongly durable) + Kafka replay |
| Ordering per correlation id | partition order | partition order (pipelined on one connection) |

## Kubernetes

- Applications are `Deployment`s (never StatefulSets), no PVCs; Kafka Streams state is an `emptyDir`
  (lost with the pod, kept across a container restart). Kafka, Redis, PostgreSQL and Prometheus use PVCs.
- Rolling updates `maxSurge: 1, maxUnavailable: 0`, `terminationGracePeriodSeconds: 45`, startup/readiness/
  liveness probes. Readiness = joined the group and running; per-task restoration is exposed separately
  (`/status`, metrics), never inferred from readiness.
- Group membership timing is the same for all protocols: session timeout 10 s, heartbeat 250 ms (ADR 0003).
- Downstream services (persistence, ML simulator) are redeployed with every run, identical for all variants.

A rollout of the three aggregator pods therefore changes the group membership six times: three joins and
three leaves. Within each step the new pod is ready before the old one is stopped; the old pod's shutdown can
overlap the next step:

```mermaid
sequenceDiagram
    participant D as Deployment controller
    participant N as new pod
    participant O as old pod
    participant G as group coordinator<br/>(Kafka broker)

    loop for each of the 3 pods (maxSurge 1, maxUnavailable 0)
        D->>N: create
        N->>G: join the group
        G-->>N: assignment
        Note over N: ready = joined and running<br/>(A: its tasks may still be restoring)
        D->>O: SIGTERM, up to 45 s grace
        O->>O: finish in-flight actions, commit
        O->>G: leave the group
        Note over G: partitions or tasks of the old pod move
    end
```
