# Impact of one disruption: late events per rollout and per pod failure

The p99.95 per window says how high the tail is, not how many events it contains. This note counts the events
that one disruption pushes over the 500 ms SLO at 6,000 events/s: one rollout, one pod failure, one Redis restart.
Counts add up, so they can be multiplied by how often each disruption happens and compared with an error budget.

All figures come from the core campaign (`results/runs`, the same runs as `docs/RESULTS.md`), computed by
`scripts/analysis/disruption_impact.py` with a 120 s horizon. Per-run values are in
`results/disruption-impact.csv`.

## Method

- **Late event**: an incoming event (fragment) whose own durable result (the INDIVIDUAL receipt) committed more
  than 500 ms after the broker appended the event. The same count is available for aggregates (FINAL, about 0.35×
  the event count) and for all durable actions (about 1.7×).
- **Impact set**: actions in flight at any time during `[fault, fault + 120 s)`.
- **Baseline**: late actions outside the impact set, per second of the rest of the measurement window, times the
  length of the impact period. The shared PostgreSQL stalls now and then in every variant
  (`docs/ANALYSIS-NOTES.md` note 2); the baseline removes the stalls that would have happened anyway.
- **Excess** = late events in the impact set − baseline, divided by the number of faults in the run (S5 has six).
- Severity (> 2 s, > 10 s) and the aggregation-only count (append → executor pickup, which persistence stalls do
  not reach) are gross counts in the impact set.

## Results

Excess late events per disruption, median [min–max] over 3 repetitions unless n=1. Load 6,000 events/s, 3 aggregator
replicas, 12 partitions.

| One disruption | A1 Streams, streams protocol | A2 Streams, classic protocol | B consumers + Redis |
|---|---|---|---|
| Rollout of all 3 aggregator pods (S2) | 180,605 [177,977–247,640] | 167,093 [155,467–226,927] | 36,927 [26,583–92,466] |
| Abrupt pod failure: SIGKILL, new pod (S3) | 140,492 [106,837–211,233] | 96,899 [84,989–157,277] | 40,727 [24,603–49,137] |
| Container crash, same pod (S3b, n=1) | 88,334 | 104,698 | 35,518 |
| Graceful pod replacement with empty state (S4) | 50,215 [47,561–129,856] | 54,036 [40,029–69,725] | 7,704 [6,021–10,265] |
| One of six replacements 45 s apart (S5) | 65,114 [55,232–78,384] | 64,418 [63,933–68,911] | 6,508 [4,417–12,120] |
| Redis restart (S8, n=1) | – | – | 208,960 |
| Redis crash losing the last 5 s of writes (S8, n=1) | – | – | 206,711 |

In seconds of traffic (median ÷ 6,000): a rollout costs 30 s (A1), 28 s (A2) and 6 s (B); an abrupt pod failure
23 s, 16 s and 7 s; a cold pod replacement 8 s, 9 s and 1.3 s; a Redis restart 35 s.

How severe and how long, medians:

| One disruption | Events > 2 s (A1 / A2 / B) | Events > 10 s (A1 / A2 / B) | Latency back under 500 ms after, s | Longest partition stall, s |
|---|---|---|---|---|
| Rollout (S2) | 117,048 / 114,053 / 12,166 | 14,677 / 1,103 / 0 | 51 / 38 / 29 | 37.6 / 32.4 / 25.5 |
| Abrupt pod failure (S3) | 125,830 / 76,876 / 28,569 | 31,791 / 17,450 / 1,217 | 36 / 32 / 17 | 21.7 / 16.0 / 12.5 |
| Cold pod replacement (S4) | 13,378 / 8,288 / 89 | 0 / 0 / 0 | 24 / 15 / 11 | 12.9 / 11.4 / 7.3 |
| Redis restart (B, n=1) | – / – / 181,946 | – / – / 65,854 | – / – / 44 | – / – / 25.7 |

No event was lost or processed twice in any of these runs (correctness PASS); the impact is delay only.

## Is the count valid decision input?

Checks made on this data:

1. **Reconciliation**: the script recomputes the verifier's failure-window over-SLO count from the raw rows; all 41
   fault runs match `verification.json` exactly.
2. **Horizon**: with 60, 90, 120 and 150 s, the severity counts do not change and the aggregation-only counts change
   by at most 3 %, so each disruption's effect is over within a minute. The end-to-end excess moves by up to about
   20 % for A and 40 % for B (B rollout 41k / 41k / 37k / 52k) because a different amount of baseline is subtracted.
3. **Additivity**: one replacement in S5 costs about the same as the single replacement in S4 (A1 65k vs 50k, A2
   64k vs 54k, B 6.5k vs 7.7k). Counts can be summed over events, but for A, replacements 45 s apart cost about
   20–30 % more each.
4. **Repetitions**: the same disruption varies up to 3.5× between repetitions (B rollout 27k–92k, A1 cold pod
   48k–130k). B's range does not overlap either A range for any aggregator disruption; A1's and A2's ranges overlap
   for every disruption.
5. **Baseline noise**: without faults this environment produces 0.5k–9.9k late events per minute (shared
   PostgreSQL). For B's smallest disruptions (cold pod, ~7.7k) the subtracted baseline (5.5k–6.7k) is as large as
   the result, so those figures are uncertain by about ±50 %. Severity and aggregation-only counts are hardly
   affected (B has no aggregation-only late aggregates in steady state).

What the numbers support:

- Per aggregator-pod disruption, **B produces 2.4–10× fewer late events** than either Kafka Streams variant
  (medians). After a rollout or an abrupt failure it also has far fewer severe ones (> 10 s: 0 and 1.2k for B,
  1.1k–32k for A).
- **A1 and A2 do not differ in count.** A2's lead in the p99.95 table (`docs/RESULTS.md`) comes from fewer severe
  delays (> 10 s after a rollout: 1.1k vs 14.7k), not from fewer late events.
- **A Redis restart costs B about 209k late events** (n=1), more than a Kafka Streams rollout. B's advantage holds
  only while Redis restarts are much rarer than aggregator rollouts, or if a Sentinel failover is cheaper than a
  restart (NOT RUN).

What they do not support:

- **Absolute numbers for production.** A count is roughly event rate × stall duration, and the stall durations
  here depend on this setup: 10 s session timeout (an abrupt failure is detected after about 10 s), small state
  (1 s correlation window, 120 s retention), 3 replicas, 12 partitions, one node with a local disk. Re-run S2–S4
  with production values before using absolute numbers for budgeting.
- **Disruptions that were not run**: Kafka broker restart and Redis Sentinel failover (NOT RUN), node failure or
  drain (impossible on a single node).

## Using it in a decision

For a p99.95 SLO evaluated over a month, the error budget is 0.05 % of 15.55 billion events (6,000/s × 30 days) =
**7.78 million late events per month**. Then

    expected late events per month = Σ (disruptions per month × late events per disruption) + steady-state late events

One disruption uses this share of the monthly budget (medians):

| One disruption | A1 | A2 | B |
|---|---|---|---|
| Rollout | 2.3 % | 2.1 % | 0.5 % |
| Abrupt pod failure | 1.8 % | 1.2 % | 0.5 % |
| Cold pod replacement | 0.6 % | 0.7 % | 0.1 % |
| Redis restart | – | – | 2.7 % |

Example with assumed frequencies (replace with your own): 20 rollouts, 2 pod failures and, for B, 1 Redis restart per
month use 50 % (A1), 45 % (A2) and 13 % (B) of the budget.

Two conditions apply:

- **Steady state must meet the SLO first.** In this environment steady state alone produces 23–428 million late
  events per month (3–55× the budget), from the shared persistence path. The calculation ranks what disruptions
  cost; it does not show the budget is met.
- **If the SLO is evaluated per short window** (for example p99.95 per 5 minutes), the count does not decide:
  every disruption breaks its window in every variant, and the measure that matters is how long the breach lasts
  (the "latency back under 500 ms" column).

To make the count decision-grade:

1. Agree on the SLO's evaluation period and on expected frequencies: rollouts, pod failures, node drains, Redis and
   Kafka upgrades.
2. Run `redis-failover` (HA profile) and `kafka-restart`, so the extra failure domains are priced in.
3. Re-run S2–S4 with production state size, timeouts and replica count.
4. Use at least 5 repetitions wherever ranges overlap (A1 vs A2).
5. Remove the persistence noise (dedicated database or faster disk), or decide on the aggregation-only count, so
   small disruptions can be resolved.

## Reproduce

Needs each run's `latencies.csv.gz`, which is not committed (about 25 MB per run):

    ./scripts/analysis/disruption_impact.py --summary results/runs/{rolling,abrupt-pod,abrupt-container,cold-pod,rescheduling,redis-restart,redis-loss}-*
    ./scripts/analysis/disruption_impact.py results/runs/{steady,rolling,abrupt-pod,abrupt-container,cold-pod,rescheduling,redis-restart,redis-loss}-* > results/disruption-impact.csv

`--horizon 60` (or another value) repeats the horizon check.
