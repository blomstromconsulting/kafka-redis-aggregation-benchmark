#!/usr/bin/env python3
"""Extracts the aggregator pods' GC pauses from a run's pod logs into gc.json.

Both builds log every collection to stdout in the same unified-logging format (JVM: -Xlog:gc, native executable:
-XX:+PrintGC), e.g. "[12.3s][info][gc] GC(41) Pause Young (Normal) (G1 Evacuation Pause) 92M->59M(128M) 3.491ms".
Only stop-the-world pauses ("Pause ...") count; concurrent phases do not stop the application. Logs of pods
replaced during the run are not collected (benchmark.sh keeps the logs of the pods alive at the end), so in fault
scenarios the figures cover the replacements, not the replaced pods.

Usage: scripts/lib/gc_pauses.py results/runs/<run> [...]   (writes <run>/gc.json)
"""
import glob
import json
import os
import re
import sys

PAUSE = re.compile(r"\]\[gc *\] GC\(\d+\) Pause .* (\d+(?:\.\d+)?)ms\s*$")


def pauses(log_file):
    with open(log_file, errors="replace") as handle:
        return [float(m.group(1)) for m in map(PAUSE.search, handle) if m]


def summary(values):
    ordered = sorted(values)
    return {
        "pauses": len(ordered),
        "maxPauseMs": round(ordered[-1], 3) if ordered else 0.0,
        "p99PauseMs": round(ordered[min(len(ordered) - 1, int(0.99 * len(ordered)))], 3) if ordered else 0.0,
        "totalPauseMs": round(sum(ordered), 1),
        "pausesOver100ms": sum(1 for v in ordered if v > 100),
    }


def main(run_dirs):
    for run_dir in run_dirs:
        logs = sorted(glob.glob(os.path.join(run_dir, "logs", "aggregator-*.log")))
        logs = [f for f in logs if not f.endswith(".previous.log")]
        if not logs:
            print(f"{run_dir}: no aggregator logs", file=sys.stderr)
            continue
        per_pod = {os.path.basename(f)[:-len(".log")]: pauses(f) for f in logs}
        if not any(per_pod.values()):
            print(f"{run_dir}: no GC log lines (GC logging not enabled for this run)", file=sys.stderr)
            continue
        data = {"source": "scripts/lib/gc_pauses.py",
                "aggregator": dict(summary([v for values in per_pod.values() for v in values]), pods=len(per_pod)),
                "pods": {pod: summary(values) for pod, values in per_pod.items()}}
        with open(os.path.join(run_dir, "gc.json"), "w") as handle:
            json.dump(data, handle, indent=2)


if __name__ == "__main__":
    main(sys.argv[1:])
