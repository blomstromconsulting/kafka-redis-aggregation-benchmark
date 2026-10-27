# ADR 0003 – Identical group membership timing for all three protocols

**Status:** accepted

**Context.** Session timeout and heartbeat interval dominate failure detection and rebalance propagation. They are
configured in different places per protocol: client configs (classic), broker/group configs (consumer, streams).
During the first smoke test the broker reported the built-in defaults (45 s session, 5 s heartbeat, 3 s initial
rebalance delay) for a new streams group although the broker-level `group.streams.*` values were 10 s / 1 s.

**Decision.** 10 s session timeout and 1 s heartbeat for every group. Classic: client config. Consumer: broker
config. Streams: broker config **and** explicit group-level configs set by `benchmark.sh` for each run; every pod
verifies the effective group configs via the Admin API at startup and exits on mismatch.

**Consequences.** Recovery differences between A1, A2 and B can be attributed to the protocols and architectures,
not to different timeouts. The values are production-plausible but tighter than Kafka's defaults; results with
default timeouts would show longer abrupt-failure stalls for every variant.
