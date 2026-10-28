# ADR 0003 – Identical group membership timing for all three protocols

**Status:** accepted

**Context.** Session timeout and heartbeat interval dominate failure detection and rebalance propagation. They are
configured in different places per protocol: client configs (classic), broker/group configs (consumer, streams).
During the first smoke test the broker reported the built-in defaults (45 s session, 5 s heartbeat, 3 s initial
rebalance delay) for a new streams group although the broker-level `group.streams.*` values were 10 s / 1 s.

**Decision.** 10 s session timeout and 250 ms heartbeat for every group (1 s until 2026-10-27: every run in
`results/archive/jdk21-hb1s`, `results/jdk25-jvm` and `results/jdk25-native`; each run's `values.yaml` records the value).
The heartbeat paces partition moves: a member that gives a partition up acknowledges at once, but the new owner
learns of its assignment only on its next heartbeat, so each move paused its partition for 0–1 s
(ANALYSIS-NOTES note 7). Classic: client config. Consumer: broker
config. Streams: broker config **and** explicit group-level configs set by `benchmark.sh` for each run; every pod
verifies the effective group configs via the Admin API at startup and exits on mismatch.

**Consequences.** Recovery differences between A1, A2 and B can be attributed to the protocols and architectures,
not to different timeouts. The values are production-plausible but tighter than Kafka's defaults (45 s session,
5 s heartbeat for consumer and streams groups); results with default timeouts would show longer abrupt-failure
stalls and partition moves for every variant. The broker's minimum heartbeat (`group.*.min.heartbeat.interval.ms`)
is lowered to the same value. Heartbeat traffic grows with members ÷ interval, which is negligible at this scale.
