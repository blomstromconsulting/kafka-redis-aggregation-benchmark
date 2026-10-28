# Fault-path validation runs (not part of the comparison)

Short runs at 1,000 fragments/s (30 s warm-up, 100–120 s measurement, one variant each) executed on 2026-10-26
to check that every fault injection, the timeline instrumentation and the verifier work end to end before the
campaign. `scripts/report.py` reads only `results/runs/`; this directory has its own `report.html`/`RESULTS.md`.

| Run | Fault | Note |
|---|---|---|
| abrupt-container-a1-r1-1026145204 | SIGKILL of the A1 container (same pod) | verifier first hung on a stale-statistics query plan; re-verified with `scripts/verify-run.sh` after the fix |
| rolling-a2-r1-1026150348 | `kubectl rollout restart` | |
| abrupt-pod-b-r1-1026150644 | **invalid as an abrupt failure**: `kubectl delete --grace-period=0 --force` alone | the kubelet still sent SIGTERM; the pod left its group gracefully within 80 ms. Led to the SIGKILL-first fix in `chaos.sh` |
| redis-loss-b-r1-1026150938 | Redis crash losing ~5 s of acknowledged writes | replay recovery exercised; correctness PASS |
| rescheduling-a1-r1-1026151225 | 2 graceful pod replacements | |
| slow-ml-b-r1-1026151539 | inference 50 → 300 ms for 60 s | |
| abrupt-pod-b-r1-1026151907 | SIGKILL + force delete (fixed) | partitions stalled ≈ session timeout (9.6 s), as expected |

All seven runs: correctness PASS.
