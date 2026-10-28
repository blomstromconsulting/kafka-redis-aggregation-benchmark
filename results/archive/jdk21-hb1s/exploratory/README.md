# Exploratory runs (not part of the comparison)

Capacity probe executed while the benchmark was being built (2026-10-26, 1 repetition each, smoke/medium/steady).
They used an earlier configuration: consumer back-pressure counted Kafka records instead of business actions
(10,000 records for both the Streams intent consumer and the Redis ingress consumer) and PostgreSQL ran without
the per-run CHECKPOINT/VACUUM. Kept for transparency; `scripts/report.py` reads only `results/runs/`.

Observations that led to the changes:
- At 6,000 fragments/s all three variants achieved the offered load with correctness PASS but missed the SLO;
  the tail episodes coincided with 1–2 s PostgreSQL stalls in the shared persistence path.
- The record-based in-flight bound paused the Streams intent consumer at fewer outstanding business actions than
  the Redis consumer (one intent = one action, one fragment = up to three actions).
