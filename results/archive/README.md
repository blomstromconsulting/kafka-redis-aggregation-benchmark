# Archived result sets

`jdk21-hb1s/` – the first campaign (2026-10-26): Java 21 JVM, 1 s group heartbeat, Kafka broker limited to 2 GiB,
OrbStack VM 14.7 GB. `runs/` holds the 68 runs that `docs/RESULTS.md` described until the final campaign of
2026-10-27/28; its report is `jdk21-hb1s/RESULTS.md` and `jdk21-hb1s/report.html`. `validation/` and
`exploratory/` are that campaign's validation and exploratory runs. Not comparable with later result sets (see
ADR 0003, ADR 0005 and `docs/ANALYSIS-NOTES.md`).

`excluded/` – runs of the final campaign left out of its reports, each with the reason:

- `redis-loss-b-r1-1027225655` (native build): the JVM redis-loss run had crashed the same Redis container 6 minutes
  earlier, so the kubelet held this crash's restart back (event `Back-off restarting failed container redis` in the
  run's local pod logs). Redis was ready 16.8 s after the crash instead of 4.4 s in the JVM run (`REDIS_READY` in
  `timeline.csv`), so the run measures that test-order delay, not the variant. Since then every redis-loss run starts with a fresh Redis pod
  (`reset_shared_state` in `scripts/benchmark.sh`); the native run was repeated as `results/native/runs/redis-loss-b-r1-*`.
