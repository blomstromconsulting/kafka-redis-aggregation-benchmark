-- Benchmark database. Written by: persistence-service (receipts, aggregates, events), load-generator
-- (runs, manifest, samples). Read by: correctness-verifier. Nothing here is written by the aggregators directly.

-- One row per benchmark run (load generator).
CREATE TABLE runs (
    run_id              text PRIMARY KEY,
    variant             text   NOT NULL,
    scenario            text   NOT NULL,
    repetition          int    NOT NULL DEFAULT 1,
    config              jsonb  NOT NULL,
    created_us          bigint NOT NULL,
    start_us            bigint,
    measure_start_us    bigint,
    measure_end_us      bigint,
    send_end_us         bigint,
    finished_us         bigint,
    planned_aggregates  bigint,
    planned_events      bigint,
    events_sent         bigint,
    events_acked        bigint,
    send_failures       bigint,
    requested_rate      double precision,
    achieved_rate       double precision,
    max_schedule_lag_ms bigint,
    drained             boolean,
    status              text   NOT NULL
);

-- Input manifest: what the load generator sent for every correlation id (independent ground truth).
CREATE TABLE manifest (
    run_id              text    NOT NULL,
    correlation_id      text    NOT NULL,
    idx                 bigint  NOT NULL,
    partition           int     NOT NULL,
    start_us            bigint  NOT NULL,
    warmup              boolean NOT NULL,
    sent_mask           int     NOT NULL, -- bit 0 = A v1 sent, bit 1 = B v1, bit 2 = C v1
    missing_type        int     NOT NULL, -- 0 = none
    duplicate_type      int     NOT NULL, -- 0 = none; duplicate of the v1 event
    stale_type          int     NOT NULL, -- 0 = none; a v0 event sent after v1
    out_of_order        boolean NOT NULL,
    requires_inference  boolean NOT NULL,
    hash_a              text,
    hash_b              text,
    hash_c              text,
    expected_stage2_hash text,
    expected_checksum   text,
    PRIMARY KEY (run_id, correlation_id)
);

CREATE TABLE send_failures (
    run_id         text NOT NULL,
    correlation_id text NOT NULL,
    event_id       text NOT NULL,
    error          text
);

-- Durable receipts of INDIVIDUAL and STAGE2 actions. Idempotency key: (run_id, action_id).
CREATE TABLE action_receipts (
    run_id             text   NOT NULL,
    action_id          text   NOT NULL,
    kind               text   NOT NULL,
    correlation_id     text   NOT NULL,
    fragment_type      int    NOT NULL,
    event_ids          text   NOT NULL,
    content_hash       text   NOT NULL,
    ingress_partition  int    NOT NULL,
    source_append_us   bigint NOT NULL,
    first_append_us    bigint NOT NULL,
    computed_us        bigint NOT NULL,
    ready_us           bigint NOT NULL,
    precommit_us       bigint NOT NULL,
    batch_id           bigint NOT NULL,
    duplicate_attempts int    NOT NULL DEFAULT 0,
    variant            text,
    pod                text,
    PRIMARY KEY (run_id, action_id)
);

-- Final aggregates. Idempotency key: (run_id, correlation_id); exactly one per complete aggregate.
CREATE TABLE aggregates (
    run_id             text   NOT NULL,
    correlation_id     text   NOT NULL,
    version            int    NOT NULL,
    event_ids          text   NOT NULL,
    checksum           text   NOT NULL,
    payload_a          text   NOT NULL,
    payload_b          text   NOT NULL,
    payload_c          text   NOT NULL,
    ml_score           double precision,
    ml_model           text,
    inference_us       bigint,
    ingress_partition  int    NOT NULL,
    source_append_us   bigint NOT NULL,
    first_append_us    bigint NOT NULL,
    computed_us        bigint NOT NULL,
    ready_us           bigint NOT NULL,
    precommit_us       bigint NOT NULL,
    batch_id           bigint NOT NULL,
    duplicate_attempts int    NOT NULL DEFAULT 0,
    variant            text,
    pod                text,
    PRIMARY KEY (run_id, correlation_id)
);

-- Commit time of every write batch, recorded right after COMMIT returned.
CREATE TABLE commit_batches (
    batch_id      bigint PRIMARY KEY,
    run_id        text,
    rows          int,
    precommit_us  bigint,
    postcommit_us bigint
);

-- Inconsistent duplicate writes that were rejected (must stay empty).
CREATE TABLE conflicts (
    id             bigserial PRIMARY KEY,
    run_id         text,
    kind           text,
    item_id        text,
    existing_hash  text,
    attempted_hash text,
    at_us          bigint
);

-- Timeline: fault injections (chaos.sh), assignment changes, restoration, Redis epochs, ...
CREATE TABLE bench_events (
    id     bigserial PRIMARY KEY,
    run_id text,
    ts_us  bigint,
    source text,
    type   text,
    detail jsonb
);
CREATE INDEX bench_events_run ON bench_events (run_id, ts_us);

-- Independent consumer-lag sampling (load generator's lag monitor).
CREATE TABLE lag_samples (
    run_id            text,
    ts_us             bigint,
    group_id          text,
    topic             text,
    lag               bigint,
    max_partition_lag bigint,
    oldest_age_ms     bigint,
    per_partition     jsonb
);
CREATE INDEX lag_samples_run ON lag_samples (run_id, ts_us);

-- Offered vs achieved producer rate, per second.
CREATE TABLE producer_samples (
    run_id              text,
    ts_us               bigint,
    phase               text,
    requested_rate      double precision,
    sent                bigint,
    acked               bigint,
    failed              bigint,
    max_schedule_lag_ms bigint
);
CREATE INDEX producer_samples_run ON producer_samples (run_id, ts_us);
