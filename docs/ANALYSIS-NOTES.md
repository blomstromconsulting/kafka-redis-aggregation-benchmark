# Analysis notes (manual, run-specific)

`docs/RESULTS.md` is generated from data and makes no causal claims. This file records manual investigations
of *why* specific runs behaved as they did, with the evidence and the commands to reproduce them. Each note
names the run it is based on; treat it as a hypothesis check, not as a benchmark result.

## 1. Kafka Streams tail latency at 6,000 fragments/s is a cluster-wide pause before executor pickup

Run: `results/archive/jdk21-hb1s/runs/steady-a1-r1-1026152213` (S1, A1). Similar pattern in the exploratory A2 run.

- Worst-path p99.95 1,020 ms; aggregation-only (append → executor pickup) p99.95 ≈ 650 ms, so most of the tail
  arises before any downstream call.
- Over-SLO actions cluster in 1–3 s episodes (`per_second_latency.csv`, column `over_slo`), roughly once a minute.
- In an episode **all 12 partitions on all three pods** are delayed simultaneously, entirely in the
  append → pickup part (raw data: `latencies.csv.gz`, columns `ready_ms` vs `done_ms`):

  ```bash
  gzip -dc results/archive/jdk21-hb1s/runs/steady-a1-*/latencies.csv.gz | awk -F, 'NR>1 && $3>=127.5 && $3<128.5 && $4>500 {p[$2]++} END {for (k in p) print k, p[k]}'
  ```
- Ruled out as the cause for this run: PostgreSQL (persistence batch mean 4–10 ms during the episodes, see
  `resources.json` series `persistenceBatchMeanSeconds`), aggregator GC (max pause 37 ms), Kafka Streams commit
  latency (≤ 121 ms), punctuation (≤ 52 ms), poll (≤ 83 ms), Kafka broker GC (max 210 ms, not in the window).
- Partially correlated: the 12 `aggregation-store` changelog partitions receive equal traffic and roll their
  64 MiB segments within ~3 s of each other (`kubectl -n aggbench exec kafka-0 -- grep "Rolled new log segment"
  /opt/kafka/logs/server.log`); the roll at 14:24:45–47 coincides with the first episode, but other episodes
  have no roll nearby.
- Interpretation: a shared-infrastructure pause (single broker / single VM disk) that every Streams task waits
  on via its EOS transaction commit and the read_committed intent consumer. Variant B does not write changelogs
  or transactions, so its exposure to the same broker is smaller; it is exposed to Redis and to PostgreSQL in
  the same way as A. A multi-broker, multi-disk cluster may behave differently — this is a single-node finding.
- Later evidence (note 2): in repetitions 2–3 the dominant S1 tail moved downstream and hit B as well, so the
  pre-pickup pauses explain only part of A's steady-state tail.
- Untested hypothesis worth checking first: in every A run the Kafka broker container sits at its 2 GiB memory
  limit (working set incl. active page cache; heap is 1 GiB), in B runs at ≈ 1.57 GiB (`resources.json`,
  `groups.kafka.memoryMaxBytes`). Page-cache reclaim or dirty-page throttling inside the broker's cgroup would
  pause changelog and transaction writes for all tasks at once.

## 2. S1 end-to-end tails in repetitions 2–3 come from the shared persistence path, in all variants

Runs: `results/archive/jdk21-hb1s/runs/steady-*-r2-*`, `steady-*-r3-*` (batch 2, 19:16–20:00 local time on 2026-10-26).

- B's S1 worst-path p99.95 rose from 651 ms (r1) to 2,077 / 2,413 ms (r2 / r3); A1 and A2 rose too. The
  over-SLO actions come in 1–2 s episodes that involve all 12 partitions.
- Splitting every over-SLO action (> 500 ms) by where the delay arose — before executor pickup
  (`ready_ms` ≥ 250 ms, i.e. aggregation) or after it (persistence / inference):

  | Run | over 500 ms | delay before pickup | delay after pickup |
  |---|---|---|---|
  | steady-a1-r1 | 25,844 | 12,488 | 13,356 |
  | steady-a1-r2 | 51,156 | 1,144 | 50,012 |
  | steady-a1-r3 | 63,160 | 9,124 | 54,036 |
  | steady-a2-r1 | 22,656 | 7,650 | 15,006 |
  | steady-a2-r2 | 50,158 | 6,525 | 43,633 |
  | steady-a2-r3 | 47,294 | 1,892 | 45,402 |
  | steady-b-r1 | 5,093 | 0 | 5,093 |
  | steady-b-r2 | 48,421 | 0 | 48,421 |
  | steady-b-r3 | 81,865 | 0 | 81,865 |

  ```bash
  for r in results/archive/jdk21-hb1s/runs/steady-*; do gzip -dc $r/latencies.csv.gz | awk -F, -v run=$(basename $r) \
    'NR>1 && $3>=0 && $5>500 {o++; if ($4>=250) up++; else down++} END {print run, o, up+0, down+0}'; done
  ```
- During the episodes the executors' persistence queue (`resources.json` series `persistenceQueueDepth`) jumps
  from ≈ 0 to 1,100–11,700 actions, and the persistence service's slowest single batch
  (`bench_persistence_batch_seconds_max`, measured around connection acquire + INSERT + COMMIT) reached 1.96 s
  in `steady-b-r2` at second 70, i.e. single PostgreSQL write transactions stalled for up to ~2 s. PostgreSQL's
  log shows only the per-run `CHECKPOINT` issued before each warm-up, no timed or WAL-triggered one; autovacuum runs
  every minute but its completions do not line up with the episodes. The underlying I/O cause on the shared VM
  disk was not identified.
- Interpretation: the S1 end-to-end comparison is dominated by a stall that every variant shares, which is why
  the generated findings report no clear S1 winner. The part that differs by design is visible in the
  aggregation-only latency (append → executor pickup, S1 steady window, p99.95): A1 412–653 ms,
  A2 348–555 ms, B 110–136 ms, and in the "delay before pickup" column above (B: none in any run).

## 3. Steady-window figures of fault scenarios depend on how actions are assigned to windows

The verifier assigns an action to a failure window by the append time of its source fragment. Actions appended
shortly before a fault that are still in flight when it hits are delayed by the fault but counted as steady.
`scripts/analysis/window_sensitivity.py` recomputes the split from `latencies.csv.gz`; its append-rule column
reproduces `verification.json` exactly for all 56 fault runs, and the overlap rule (in flight at any time during
the window) changes the steady-window worst-path p99.95 by more than 5 % in three runs:

| Run | append rule (reported) | overlap rule |
|---|---|---|
| abrupt-pod-a1-r2-1026205656 | 12,454 ms | 3,102 ms |
| correctness-a1-r1-1026184718 | 9,980 ms | 275 ms |
| state-60s-a2-r1-1026181211 | 1,329 ms | 1,176 ms |

```bash
scripts/analysis/window_sensitivity.py results/archive/jdk21-hb1s/runs/abrupt-pod-* results/archive/jdk21-hb1s/runs/correctness-*
```

The remaining 3.1 s in `abrupt-pod-a1-r2` comes from a 7 s episode 30 s before the kill (seconds 29–37,
all 12 partitions, mean pickup delay ≈ 0.7 s, i.e. before the executor — the pattern of note 1). Whole-window
figures, which the main comparison uses for S2–S5, are not affected. The verifier keeps the append rule so that
all campaign runs are evaluated identically.

## 4. Provenance: resource figures were re-exported after the campaign

The first campaign export added the previous run's pods to the aggregator memory totals (kubelet samples carry
explicit timestamps, so Prometheus returned deleted pods' last values for five minutes; e.g. A2 S1 showed
4.6 GiB for three pods limited to 1.25 GiB each). After fixing `scripts/lib/prom_export.py`
(see BENCHMARK-METHODOLOGY, Limitations), `resources.json` of every run in `results/archive/jdk21-hb1s/runs`, `.../validation`
and `.../exploratory` (then `results/runs`, `results/validation`, `results/exploratory`) was re-exported from the same Prometheus data on 2026-10-26 (retention 3 days).
Only the resource group figures changed (aggregator memory in all runs; CPU and memory totals in runs where pods
were replaced); timeline series differ at most by a few trailing points that had not been scraped yet when the
original export ran.

## 5. In variant B, a rollout's late events come from the new pods' first 10–25 s, not from moving partitions

Runs: `results/archive/jdk21-hb1s/runs/rolling-b-r{1,2,3}-*` (S2, B, Java 21 JVM). Every late event (INDIVIDUAL action over 500 ms)
appended in the minute after the rollout started is attributed to the pod that picked it up and to the stage where
the time went (`scripts/analysis/late_event_causes.py`):

| Where the time went | r1 | r2 | r3 |
|---|---|---|---|
| New pod: picked up within the SLO, slow in the executor → persistence path | 64.3 % | 62.8 % | 65.0 % |
| New pod: slow to pick the event up | 24.1 % | 26.3 % | 20.5 % |
| Handover: partition moving between pods | 10.5 % | 10.8 % | 13.4 % |
| Pods that were already running | 1.0 % | 0.0 % | 1.1 % |
| Late events in total | 95,847 | 42,520 | 45,699 |

- New-pod late events fall in the pod's first 10–15 s (r2, r3) or 25 s (r1).
- The rollout replaces one pod at a time (maxSurge 1, maxUnavailable 0); each old pod stops 1.5–3 s after its
  replacement starts, because readiness only requires the consumer to have joined and polled. Kafka moves
  partitions to a new pod within about a second of it joining, whatever its readiness. In r1, 18 partition moves
  for 12 partitions (each join and each leave rebalances).
- The bottleneck is inside the new pod: its outgoing queues (`persistenceQueueDepth`, summed over pods) rise from
  about 20 to 6,000–18,000 (r1) or 2,500–5,300 (r2), and the inference queue from 0 to 1,000–4,100, while the
  persistence service's batch time stays at about 10 ms and running pods' requests to the same services stay fast.
  Back-pressure never engaged (`consumersPaused` 0).
- New pods peak at 1.3–1.4 CPU cores (30 s rate) against about 0.8 for running pods doing the same share of the
  work; aggregator CPU in total rises from 2.3 to 4.1–4.6 cores during the rollout.
- A partition moved to a running pod resumes with its oldest waiting event 0.2–0.3 s old, one moved to a new pod
  with 1–4 s. Giving a partition up takes 2.0–2.9 s from a new pod and at most 0.2 s from a running one.
- The same holds for S4 (cold pod, r2: new-pod warm-up 81 %, handover 18 %). In S3 (abrupt pod, r2) half of the
  late events are handovers, i.e. the 10 s session timeout.

Hypothesis, consistent with the above but not proven: JIT warm-up of a freshly started JVM (interpreted and lightly
compiled code, compiler threads competing for the 2-core limit). CPU throttling was not exported and is not
verified. Test: ADR 0005 (native executables have no warm-up phase).

Reproduce: `./scripts/analysis/late_event_causes.py results/archive/jdk21-hb1s/runs/rolling-b-r*` (needs `latencies.csv.gz`, local
only); queue and CPU series are in each run's `resources.json` (`series`, `pods`).

## 6. Theory test: native executables remove B's rollout delay; A gains little

Runs (2026-10-27, selected scenarios only, not the full suite): `results/jdk25-native` (aggregators as native
executables, Oracle GraalVM 25.0.5, G1; ADR 0005) and `results/jdk25-jvm` (Java 25 JVM baseline, Temurin 25.0.4.1).
Native: S2 rolling ×3, S1 steady ×1, smoke and correctness ×1 per variant; JVM 25: S2 ×1, S1 ×1 per variant. All
24 runs: correctness PASS, load achieved. The OrbStack VM was enlarged (14.7 → 18.9 GB, restarted) before these
runs, so only JVM 25 vs native is like-for-like; the Java 21 campaign is shown for context.

S2 rolling, per rollout, median [min–max] (`disruption_impact.py`, 120 s horizon; p99.95 is the worst path over the
whole measurement window):

| | Java 21 JVM (n=3) | Java 25 JVM (n=1) | Native (n=3) |
|---|---|---|---|
| **B** late events added by the rollout | 36,927 [26,583–92,466] | 20,776 | 852 [−923–6,982] |
| B events > 2 s | 12,166 [1,584–55,587] | 299 | 0 [0–0] |
| B p99.95, ms | 2,955 [2,940–5,465] | 1,944 | 1,092 [923–1,337] |
| B latency back under 500 ms after, s | 29 [29–39] | 26 | 13 [13–14] |
| **A1** late events added by the rollout | 180,605 [177,977–247,640] | 74,921 | 85,931 [41,622–141,148] |
| A1 events > 10 s | 14,677 [14,343–45,348] | 0 | 2,840 [0–5,314] |
| A1 p99.95, ms | 26,893 [21,792–37,010] | 8,598 | 13,460 [5,514–16,607] |
| **A2** late events added by the rollout | 167,093 [155,467–226,927] | 87,802 | 63,893 [29,985–75,630] |
| A2 events > 10 s | 1,103 [0–11,074] | 0 | 0 [0–0] |
| A2 p99.95, ms | 10,170 [8,734–12,054] | 4,733 | 6,603 [5,173–7,012] |
| Aggregator CPU, peak during the rollout, cores (A1 / A2 / B) | 4.8–5.2 / 4.3–5.3 / 4.5–4.6 | 4.5 / 4.2 / 4.4 | 1.5–1.8 / 1.5–1.9 / 1.5–1.9 |

B, where the late events went (`late_event_causes.py`, appended in [60, 120) s): native 2,109 / 2,559 / 7,283 (mostly
handovers; in the third run 28 % fell on running pods, i.e. a persistence stall), against 16,159 for Java 25 (75 %
on new pods, all in their first 10 s) and 42,520–95,847 for Java 21 (87–89 % on new pods).

S1 steady, one run each (worst-path p99.95; aggregation-only p99.95; aggregator CPU average; aggregator memory peak):

| | Java 25 JVM | Native |
|---|---|---|
| A1 | 4,816 ms; 731 ms; 1.45 cores; 3,567 MiB | 1,165 ms; 293 ms; 1.24 cores; 2,065 MiB |
| A2 | 491 ms (SLO PASS); 219 ms; 1.10 cores; 3,395 MiB | 1,061 ms; 300 ms; 1.04 cores; 2,228 MiB |
| B | 1,521 ms; 454 ms; 1.56 cores; 1,350 MiB | 831 ms; 39 ms; 1.48 cores; 279 MiB |

Reading:
- **B: the hypothesis of note 5 holds.** Without JIT warm-up, a rollout adds almost no late events: what remains
  are the partitions in transit. New pods no longer need 4+ extra CPU cores during the rollout.
- **A1/A2: no clear gain over Java 25 on the JVM.** Their rollout cost is dominated by rebalancing and state
  restoration, which native code does not shorten. The ranges are wide and the JVM 25 baseline has one run each.
- **Steady state: no penalty found** (one run each): equal or lower CPU, much less memory, B's aggregation step
  faster. End-to-end tails are still set by the shared persistence path in all builds.
- **Java 25 / the larger VM also changed the JVM results** (B 36,927 → 20,776, A1 180,605 → 74,921 late events per
  rollout); the two causes cannot be separated with these runs.

Open points before the full suite:
- Native executables export no GC pause metrics (Micrometer's JVM GC binder has no data in native mode); the JVM 25
  B run had a 395 ms GC pause. Enable `-XX:+PrintGC` in `nativeOpts` (or a JFR-free equivalent) to measure them.
- Native A2 r1 had aggregation stalls 100–230 s after its rollout (no rebalance, rising CPU); not seen in the other
  two native A2 runs, not explained.
- The JVM 25 baseline needs as many repetitions as the native runs.

Reproduce: the run commands are in README ("Native aggregators"); the tables come from
`./scripts/analysis/disruption_impact.py`, `./scripts/analysis/late_event_causes.py` and each run's
`verification.json` / `resources.json`.

## 7. A 250 ms heartbeat removes the partition-move pauses of B's rollouts

Runs: `results/jdk25-native-hb250` (2026-10-27, native build, S2 rolling: B ×3, A1 ×1, A2 ×1) against the native
1 s runs of note 6. The only change is the group heartbeat (ADR 0003), applied to every group; the broker was
restarted to lower its minimum heartbeat.

Why: after note 6, B's remaining rollout delay was the partitions in transit. Per move (18 per rollout), the old owner
drains its in-flight work (native: median 22 ms) and acknowledges at once; the new owner then learns of the
partition only on its next heartbeat, so the wait was uniform between 0 and the 1 s interval.

| B, native, per partition move | 1 s heartbeat (3 runs) | 250 ms heartbeat (3 runs) |
|---|---|---|
| Wait for the new owner's assignment, median / max | 0.41–0.51 s / 0.95–0.99 s | 0.10–0.15 s / 0.21–0.24 s |
| Oldest waiting event when processing resumed, median / max | 0.47–0.57 s / 1.04–1.12 s | 0.18–0.23 s / 0.29–0.39 s |
| Late events caused by moves (`late_event_causes.py`, handover) | 2,039 / 2,003 / 2,221 | 0 / 0 / 0 |
| Late aggregates before executor pickup (aggregation-only) | 491 / 505 / 635 | 0 / 0 / 0 |
| Latency back under 500 ms after the rollout started | 13–14 s | 4–8 s |

All three B runs: no late event from the rollout itself and none over 2 s. Every remaining late event of the minute
after the rollout started was delayed after pickup, on running and new pods alike, i.e. by the shared persistence
path (the excess column of `disruption_impact.py`, 5,887–10,853, is therefore persistence noise here, not
rollout cost).

A1 and A2 (one run each): 74,815 and 94,260 late events added per rollout, against 41,622–141,148 and
29,985–75,630 at 1 s. Their cost is rebalancing and state restoration; no gain is visible, and A2's single run is
above its 1 s range (to be checked with repetitions in the full suite).

Reproduce: `./scripts/analysis/late_event_causes.py results/jdk25-native-hb250/runs/rolling-b-r*`; move timings come
from the PARTITIONS_REVOKED (`revokeMs`), PARTITIONS_ASSIGNED and PARTITION_PROCESSING_STARTED (`firstRecordAgeMs`)
timeline events in each run's `timeline.csv`.
