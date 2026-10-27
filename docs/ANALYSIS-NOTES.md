# Analysis notes (manual, run-specific)

`docs/RESULTS.md` is generated from data and makes no causal claims. This file records manual investigations
of *why* specific runs behaved as they did, with the evidence and the commands to reproduce them. Each note
names the run it is based on; treat it as a hypothesis check, not as a benchmark result.

## 1. Kafka Streams tail latency at 6,000 fragments/s is a cluster-wide pause before executor pickup

Run: `results/runs/steady-a1-r1-1026152213` (S1, A1). Similar pattern in the exploratory A2 run.

- Worst-path p99.95 1,020 ms; aggregation-only (append → executor pickup) p99.95 ≈ 650 ms, so most of the tail
  arises before any downstream call.
- Over-SLO actions cluster in 1–3 s episodes (`per_second_latency.csv`, column `over_slo`), roughly once a minute.
- In an episode **all 12 partitions on all three pods** are delayed simultaneously, entirely in the
  append → pickup part (raw data: `latencies.csv.gz`, columns `ready_ms` vs `done_ms`):

  ```bash
  gzip -dc results/runs/steady-a1-*/latencies.csv.gz | awk -F, 'NR>1 && $3>=127.5 && $3<128.5 && $4>500 {p[$2]++} END {for (k in p) print k, p[k]}'
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

Runs: `results/runs/steady-*-r2-*`, `steady-*-r3-*` (batch 2, 19:16–20:00 local time on 2026-10-26).

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
  for r in results/runs/steady-*; do gzip -dc $r/latencies.csv.gz | awk -F, -v run=$(basename $r) \
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
scripts/analysis/window_sensitivity.py results/runs/abrupt-pod-* results/runs/correctness-*
```

The remaining 3.1 s in `abrupt-pod-a1-r2` comes from a 7 s episode 30 s before the kill (seconds 29–37,
all 12 partitions, mean pickup delay ≈ 0.7 s, i.e. before the executor — the pattern of note 1). Whole-window
figures, which the main comparison uses for S2–S5, are not affected. The verifier keeps the append rule so that
all campaign runs are evaluated identically.

## 4. Provenance: resource figures were re-exported after the campaign

The first campaign export added the previous run's pods to the aggregator memory totals (kubelet samples carry
explicit timestamps, so Prometheus returned deleted pods' last values for five minutes; e.g. A2 S1 showed
4.6 GiB for three pods limited to 1.25 GiB each). After fixing `scripts/lib/prom_export.py`
(see BENCHMARK-METHODOLOGY, Limitations), `resources.json` of every run in `results/runs`, `results/validation`
and `results/exploratory` was re-exported from the same Prometheus data on 2026-10-26 (retention 3 days).
Only the resource group figures changed (aggregator memory in all runs; CPU and memory totals in runs where pods
were replaced); timeline series differ at most by a few trailing points that had not been scraped yet when the
original export ran.
