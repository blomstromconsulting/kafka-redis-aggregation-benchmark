# Impact of one disruption: late events per rollout and per pod failure

The p99.95 per window says how high the tail is, not how many events it contains. This note counts the events
that one disruption pushes over the 500 ms SLO at 6,000 events/s: one rollout, one pod failure, one Redis restart.
Counts add up, so they can be multiplied by how often each disruption happens and compared with an error budget.

All figures come from the final campaign (2026-10-27/28: Java 25, 250 ms group heartbeat, Kafka broker limited to
4 GiB), run once with each aggregator build: JVM `results/runs` (report `docs/RESULTS.md`) and native
`results/native/runs` (report `results/native/RESULTS.md`). They are computed by `scripts/analysis/disruption_impact.py`
with a 120 s horizon; every run saves its figures as `disruption.json` (written by `benchmark.sh` after
verification), the reports show them per variant in the tables **Late events per disruption**, and
`results/disruption-impact.csv` and `results/native/disruption-impact.csv` list them per run. The Java 21 figures that
this note showed before are in `results/archive/jdk21-hb1s/disruption-impact.csv`.

## Method

- **Late event**: an incoming event (fragment) whose own result (the INDIVIDUAL action) was later than 500 ms after
  the broker appended the event. Two bases:
  - **result ready** (primary): until the result exists, before it is persisted (executor pickup; for final
    aggregates pickup + ML inference). In production the result is published to a Kafka topic and a Redis feed, not
    written to the benchmark's PostgreSQL, so the persistence step is not part of the SLO.
  - **end to end**: until the durable PostgreSQL commit, for comparison. It includes the stalls of the shared database.

  The same counts exist for aggregates (FINAL, about 0.35× the event count) and for all actions (about 1.7×).
- **Impact set**: actions in flight at any time during `[fault, fault + 120 s)`.
- **Baseline**: late actions outside the impact set, per second of the rest of the measurement window, times the
  length of the impact period. End to end, the shared PostgreSQL stalls now and then in every variant
  (`docs/ANALYSIS-NOTES.md` note 2); on the result-ready basis there is almost no baseline (see check 5).
- **Excess** = late events in the impact set − baseline, divided by the number of faults in the run (S5 has six).
- Severity (> 2 s, > 10 s) is a gross count in the impact set.

## Results

Late events added per disruption, **result ready**, median [min–max] over 3 repetitions unless n=1. Load 6,000
events/s, 3 aggregator replicas, 12 partitions.

Java 25 JVM:

| One disruption | A1 Streams, streams protocol | A2 Streams, classic protocol | B consumers + Redis |
|---|---|---|---|
| Rollout of all 3 aggregator pods (S2) | 53,071 [45,577–53,476] | 40,693 [15,809–63,607] | 2,806 [2,656–5,918] |
| Abrupt pod failure: SIGKILL, new pod (S3) | 39,356 [32,350–52,184] | 62,334 [62,134–67,008] | 21,592 [21,187–21,792] |
| Container crash, same pod (S3b, n=1) | 31,859 | 68,034 | 22,242 |
| Graceful pod replacement with empty state (S4) | 20,799 [18,788–27,357] | 16,103 [7,509–46,280] | 1,073 [18–2,566] |
| One of six replacements 45 s apart (S5) | 21,096 [18,077–25,335] | 12,789 [11,665–14,481] | 2,075 [1,915–2,122] |
| Redis restart (S8, n=1) | – | – | 115,328 |
| Redis crash losing the last 5 s of writes (S8, n=1) | – | – | 121,546 |

Native executables:

| One disruption | A1 Streams, streams protocol | A2 Streams, classic protocol | B consumers + Redis |
|---|---|---|---|
| Rollout of all 3 aggregator pods (S2) | 33,303 [28,114–74,150] | 23,962 [13,940–28,342] | 0 [−1,233–14] |
| Abrupt pod failure: SIGKILL, new pod (S3) | 43,286 [32,681–50,624] | 71,747 [59,907–74,301] | 19,685 [19,504–19,945] |
| Container crash, same pod (S3b, n=1) | 41,572 | 66,627 | 20,267 |
| Graceful pod replacement with empty state (S4) | 11,370 [8,008–21,676] | 6,061 [3,820–9,586] | 0 [0–0] |
| One of six replacements 45 s apart (S5) | 14,402 [11,012–14,955] | 7,418 [7,184–7,804] | 39 [0–159] |
| Redis restart (S8, n=1) | – | – | 119,830 |
| Redis crash losing the last 5 s of writes (S8, n=1) | – | – | 113,038 |

In seconds of traffic (median ÷ 6,000), JVM / native: a rollout costs 8.8 / 5.6 s (A1), 6.8 / 4.0 s (A2) and
0.5 / 0 s (B); an abrupt pod failure 6.6 / 7.2 s, 10.4 / 12.0 s and 3.6 / 3.3 s; a cold pod replacement 3.5 / 1.9 s,
2.7 / 1.0 s and 0.2 / 0 s; a Redis restart 19 / 20 s.

How severe and how long, result ready, medians, JVM / native:

| One disruption | Events > 2 s | Events > 10 s | Latency back under 500 ms after, s | Longest partition stall, s |
|---|---|---|---|---|
| Rollout (S2), A1 | 31,340 / 13,692 | 0 / 0 | 32 / 20 | 7.6 / 7.3 |
| Rollout (S2), A2 | 8,918 / 3,255 | 0 / 0 | 25 / 16 | 4.6 / 2.5 |
| Rollout (S2), B | 0 / 0 | 0 / 0 | 23 / 0 | 0 / 0 |
| Abrupt pod failure (S3), A1 | 31,945 / 38,979 | 2,709 / 10,756 | 13 / 16 | 12.0 / 15.1 |
| Abrupt pod failure (S3), A2 | 50,926 / 59,867 | 8,353 / 15,092 | 16 / 19 | 13.8 / 17.4 |
| Abrupt pod failure (S3), B | 17,315 / 16,626 | 465 / 173 | 12 / 11 | 10.6 / 10.2 |
| Cold pod replacement (S4), A1 | 6,625 / 1,710 | 0 / 0 | 12 / 9 | 4.6 / 2.7 |
| Cold pod replacement (S4), A2 | 95 / 6 | 0 / 0 | 8 / 4 | 2.1 / 0 |
| Cold pod replacement (S4), B | 0 / 0 | 0 / 0 | 6 / 0 | 0 / 0 |
| Redis restart (S8), B, n=1 | 91,836 / 94,840 | 4,076 / 4,831 | 23 / 23 | 9.6 / 10.2 |
| Redis crash with loss (S8), B, n=1 | 95,801 / 90,219 | 5,231 / 1,379 | 24 / 22 | 8.9 / 7.8 |

“Back under 500 ms” is the first second after the fault from which the p99 of every second stays at or under
500 ms for 10 seconds in a row (S5: the slowest of the six). B's JVM rollout needs 23 s although no event took longer
than 2 s, because the three pods are replaced one after another and single seconds of each new pod's JVM warm-up
have a p99 above 500 ms; the native rollout has none.

End to end (to the database commit), median, JVM / native:

| One disruption | A1 | A2 | B |
|---|---|---|---|
| Rollout (S2) | 75,649 / 46,169 | 56,297 / 43,583 | 5,163 / −4,109 |
| Abrupt pod failure (S3) | 44,977 / 48,662 | 68,673 / 73,757 | 22,912 / 27,037 |
| Container crash (S3b, n=1) | 27,481 / 42,248 | 71,433 / 67,319 | 38,119 / 17,290 |
| Cold pod replacement (S4) | 25,771 / 25,147 | 19,541 / 10,012 | 5,915 / −3,176 |
| One of six replacements (S5) | 30,559 / 21,496 | 23,948 / 11,887 | −5,010 / 1,931 |
| Redis restart / crash with loss (S8, n=1) | – | – | 114,350 / 121,432; 140,487 / 113,683 |

A negative value means the impact period had fewer late events than the run's baseline predicted: end to end, the
shared PostgreSQL produces 0–20,000 late events per minute without any fault, which hides B's small disruptions.

No event was lost or processed twice in any of the 136 runs (correctness PASS); the impact is delay only.

## Is the count valid decision input?

Checks made on this data:

1. **Reconciliation**: the script recomputes the verifier's failure-window over-SLO count from the raw rows; all 41
   fault runs of each build match `verification.json` exactly, on both bases.
2. **Horizon**: with 60, 90, 120 and 150 s, the severity counts do not change and the result-ready excess changes by
   at most 5 %, except A1's single JVM container-crash run (36k at 60 s, 28k at 150 s) and A2's native cold
   replacements (6.1k–9.6k), where a different amount of baseline is subtracted. Each disruption's effect is over
   within a minute. End to end, B's small figures move by up to about 2× (JVM rollout 7.9k / 7.8k / 5.2k / 11.9k).
3. **Additivity**: one replacement in S5 costs about the same as the single replacement in S4 (JVM: A1 21k vs 21k,
   A2 13k vs 16k, B 2.1k vs 1.1k; native: A1 14k vs 11k, A2 7.4k vs 6.1k, B 0.04k vs 0). Counts can be summed
   over disruptions 45 s apart.
4. **Repetitions**: the same disruption varies up to 6× between repetitions (A2 JVM cold pod 7.5k–46k, A2 JVM
   rollout 16k–64k, A1 native rollout 28k–74k). B's range does not overlap either A range for any aggregator
   disruption, on either build. A1 and A2 overlap for rollouts and cold replacements; A1 is lower for abrupt
   failures and A2 for rescheduling, on both builds, with ranges that do not overlap.
5. **Baseline noise**: on the result-ready basis B had no late event at all in any steady run on either build, and
   the A variants 0–1,800 per minute. End to end, the shared PostgreSQL adds up to 20,000 per minute in every variant
   (medians of the steady runs 1,100–9,100), which made B's small disruptions unresolvable on that basis (negative or near-zero medians); on the result-ready
   basis they resolve (B native rollout 0 [−1,233–14]).

What the numbers support (result ready):

- **Planned pod moves cost B almost nothing.** For a rollout, a cold replacement or rescheduling, B produces 6–15×
  fewer late events than the better Kafka Streams variant on the JVM (2.8k vs 41k, 1.1k vs 16k, 2.1k vs 13k) and
  none to speak of as a native executable (0, 0, 39). None of B's late events in these moves took more than 2 s.
- **An abrupt failure costs every variant at least the failure detection.** The killed pod's 4 of 12 partitions
  (2,000 events/s) wait for the 10 s session timeout, which is about 20,000 events: B's whole cost (20–22k). The
  Kafka Streams variants add their task migration on top (A1 39–43k, A2 62–72k). The native build does not help
  here; a shorter session timeout would help all variants.
- **A1 and A2 trade places.** A1 (streams protocol) costs less on an abrupt failure (39k vs 62k JVM, 43k vs 72k
  native), A2 (classic) less on rescheduling (13k vs 21k, 7.4k vs 14k) and with fewer severe delays in a rollout
  (> 2 s: 8.9k vs 31k JVM). Neither is better overall.
- **A Redis restart costs B about 115–120k late events** (n=1 per build), as much as two to five Kafka Streams
  rollouts; a crash losing the last 5 s costs about the same (113–122k). B's advantage holds while Redis restarts are
  much rarer than aggregator disruptions, or if a Sentinel failover is cheaper than a restart (NOT RUN).
- **Native executables lower the cost of planned moves** for every variant (rollout A1 53k → 33k, A2 41k → 24k,
  B 2.8k → 0; cold replacement 21k → 11k, 16k → 6.1k, 1.1k → 0), not of abrupt failures (ranges overlap).

What they do not support:

- **Absolute numbers for production.** A count is roughly event rate × stall duration, and the stall durations
  here depend on this setup: 10 s session timeout (an abrupt failure is detected after about 10 s), 250 ms group
  heartbeat, small state (1 s correlation window, 120 s retention), 3 replicas, 12 partitions, one node with a
  local disk. Re-run S2–S4 with production values before using absolute numbers for budgeting.
- **The publishing step.** Result ready ends before the result is published; production publishes it to a Kafka
  topic and a Redis feed. In A the transactional Kafka hop to the executor is already included, in B no Kafka
  publish at all.
- **Disruptions that were not run**: Kafka broker restart and Redis Sentinel failover (NOT RUN), node failure or
  drain (impossible on a single node).

## Using it in a decision

For a p99.95 SLO evaluated over a month, the error budget is 0.05 % of 15.55 billion events (6,000/s × 30 days) =
**7.78 million late events per month**. Then

    expected late events per month = Σ (disruptions per month × late events per disruption) + steady-state late events

One disruption uses this share of the monthly budget (result ready, medians, JVM / native):

| One disruption | A1 | A2 | B |
|---|---|---|---|
| Rollout | 0.68 % / 0.43 % | 0.52 % / 0.31 % | 0.04 % / 0.00 % |
| Abrupt pod failure | 0.51 % / 0.56 % | 0.80 % / 0.92 % | 0.28 % / 0.25 % |
| Cold pod replacement | 0.27 % / 0.15 % | 0.21 % / 0.08 % | 0.01 % / 0.00 % |
| Redis restart | – | – | 1.48 % / 1.54 % |

Example with assumed frequencies (replace with your own): 20 rollouts, 2 pod failures and, for B, 1 Redis restart per
month use 14.7 % / 9.7 % (A1), 12.1 % / 8.0 % (A2) and 2.8 % / 2.0 % (B) of the budget; the Redis restart is half
(JVM) to three quarters (native) of B's share.

Two conditions apply:

- **Steady state must meet the SLO first.** On the result-ready basis B produced no late events in steady state on
  either build. The A variants did, unevenly (median per month JVM / native: A1 15.9M / 7.6M, A2 4.5M / 0.65M, i.e.
  2.0× / 1.0× and 0.6× / 0.08× the budget), with single steady runs up to 10× the budget (JVM A2 77.7M). End to end,
  every variant uses 6–51× the budget in steady state from the shared persistence path alone.
- **If the SLO is evaluated per short window** (for example p99.95 per 5 minutes), the count does not decide:
  every abrupt failure breaks its window in every variant, and the measure that matters is how long the breach lasts
  (the "latency back under 500 ms" column).

To make the count decision-grade:

1. Agree on the SLO's evaluation period and on expected frequencies: rollouts, pod failures, node drains, Redis and
   Kafka upgrades.
2. Run `redis-failover` (HA profile) and `kafka-restart`, so the extra failure domains are priced in.
3. Re-run S2–S4 with production state size, timeouts and replica count.
4. Use at least 5 repetitions wherever ranges overlap (A1 vs A2 for rollouts and cold replacements).
5. Measure the production publishing step (Kafka topic, Redis feed), which result ready leaves out.

## Reproduce

Needs each run's `latencies.csv.gz`, which is not committed (about 25 MB per run):

    ./scripts/analysis/disruption_impact.py --summary results/runs/{rolling,abrupt-pod,abrupt-container,cold-pod,rescheduling,redis-restart,redis-loss}-*
    ./scripts/analysis/disruption_impact.py results/runs/{steady,rolling,abrupt-pod,abrupt-container,cold-pod,rescheduling,redis-restart,redis-loss}-* > results/disruption-impact.csv

The same with `results/native/runs` and `results/native/disruption-impact.csv` for the native build. `--horizon 60`
(or another value) repeats the horizon check; columns prefixed `result_` are the result-ready basis.
