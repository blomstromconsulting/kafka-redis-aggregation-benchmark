#!/usr/bin/env python3
"""Sensitivity of the steady/failure latency split to how an action is assigned to a fault window.

The verifier assigns an action to a failure window by the broker append time of the fragment it depends on
("append rule": fault <= append < fault + window). An action appended shortly *before* a fault that is still
pending when the fault hits is therefore counted as steady, although its latency is caused by the fault.
This script recomputes the split from a run's latencies.csv.gz with both rules:

  append rule   fault <= append < fault + window                (what verification.json reports)
  overlap rule  append < fault + window and completion >= fault (in flight at any time during the window)

and prints the worst-path p99.95 (highest p99.95 of INDIVIDUAL / STAGE2 / FINAL, nearest rank) for each.
The append-rule column reproduces verification.json and serves as a check of this script.

Usage: scripts/analysis/window_sensitivity.py results/runs/<run> [...]
"""
import gzip
import json
import math
import os
import sys

KINDS = ("INDIVIDUAL", "STAGE2", "FINAL")
WINDOW_SECONDS = 60.0  # the verifier's --fault-window-seconds default


def nearest_rank(sorted_values, quantile):
    if not sorted_values:
        return None
    return sorted_values[min(len(sorted_values) - 1, max(0, math.ceil(quantile * len(sorted_values)) - 1))]


def worst_path(samples):
    values = [nearest_rank(sorted(samples[k]), 0.9995) for k in KINDS if samples[k]]
    return max(values) if values else None


def env_value(path, key):
    with open(path) as handle:
        for line in handle:
            if line.startswith(key + "="):
                return line.split("=", 1)[1].strip().strip('"')
    return None


def analyse(run_dir):
    verification = json.load(open(os.path.join(run_dir, "verification.json")))
    faults = [f["atSecond"] for f in verification.get("faultRecovery") or []]
    if not faults:
        return None
    # The verifier classifies with a fixed window (faultRecovery[].windowSeconds is that window cut short by the
    # next fault, which is only used for the recovery analysis).
    window = WINDOW_SECONDS
    duration = float(env_value(os.path.join(run_dir, "scenario.env"), "DURATION_SECONDS"))
    split = {rule: {w: {k: [] for k in KINDS} for w in ("steady", "failure")} for rule in ("append", "overlap")}
    with gzip.open(os.path.join(run_dir, "latencies.csv.gz"), "rt") as handle:
        column = {name: i for i, name in enumerate(next(handle).rstrip("\n").split(","))}
        for line in handle:
            fields = line.rstrip("\n").split(",")
            kind, append_s, done_ms = fields[column["kind"]], fields[column["append_s"]], fields[column["done_ms"]]
            append_s = float(append_s)
            if append_s < 0 or append_s >= duration:
                continue
            done_s = append_s + float(done_ms) / 1000
            by_append = any(f <= append_s < f + window for f in faults)
            by_overlap = any(append_s < f + window and done_s >= f for f in faults)
            split["append"]["failure" if by_append else "steady"][kind].append(float(done_ms))
            split["overlap"]["failure" if by_overlap else "steady"][kind].append(float(done_ms))
    reported = verification["latency"]
    return {
        "run": os.path.basename(os.path.normpath(run_dir)),
        "verifier_steady": max((reported["steady"][k]["p99_95"] for k in KINDS if k in reported["steady"]), default=None),
        "append_steady": worst_path(split["append"]["steady"]),
        "overlap_steady": worst_path(split["overlap"]["steady"]),
        "overlap_steady_count": sum(len(v) for v in split["overlap"]["steady"].values()),
        "append_failure": worst_path(split["append"]["failure"]),
        "overlap_failure": worst_path(split["overlap"]["failure"]),
    }


def main(paths):
    print("run,verifier_steady_ms,append_rule_steady_ms,overlap_rule_steady_ms,overlap_rule_steady_samples,"
          "append_rule_failure_ms,overlap_rule_failure_ms")
    for path in paths:
        if not os.path.exists(os.path.join(path, "latencies.csv.gz")):
            print(f"{os.path.basename(os.path.normpath(path))},latencies.csv.gz missing", file=sys.stderr)
            continue
        row = analyse(path)
        if row:
            print(",".join(str(round(v, 2)) if isinstance(v, float) else str(v) for v in row.values()))


if __name__ == "__main__":
    main(sys.argv[1:])
