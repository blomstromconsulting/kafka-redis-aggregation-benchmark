#!/usr/bin/env python3
"""Late actions caused by one disruption (a rollout, a pod failure, ...), counted per event.

Counts actions whose end-to-end latency exceeded the SLO while a disruption was in progress, and subtracts what the
same run produces without one (the shared persistence path stalls now and then in every variant, see
docs/ANALYSIS-NOTES.md note 2):

  impact set     actions in flight during [fault, fault + horizon) for any fault: append < fault + horizon and
                 completion >= fault (the overlap rule of window_sensitivity.py)
  baseline rate  late actions outside the impact set per second of measurement outside the impact period
  excess         late actions in the impact set - baseline rate x length of the impact period

Every figure is divided by the number of faults in the run (S5 has six). Severity (> 2 s, > 10 s) and the
aggregation-only count (append -> executor pickup, which persistence stalls do not reach) are gross counts in the
impact set. As a check of the input, the verifier's failure-window over-SLO count (append rule, 60 s) is recomputed
and a difference from verification.json is reported on stderr.

Usage: scripts/analysis/disruption_impact.py [--horizon 120] [--summary | --write] results/runs/<run> [...]
  --write saves each run's figures as <run>/disruption.json (benchmark.sh does this after every run; the report
  reads these files, so it does not need the local-only latencies.csv.gz).
"""
import argparse
import gzip
import json
import multiprocessing
import os
import statistics
import sys

KINDS = ("INDIVIDUAL", "STAGE2", "FINAL")
SLO_MS = 500.0
VERIFIER_WINDOW_SECONDS = 60.0
COLUMNS = ("late_events", "baseline_events", "excess_events", "excess_aggregates", "excess_actions",
           "events_over_2s", "events_over_10s", "aggregation_only_late_aggregates")


def env_value(path, key):
    with open(path) as handle:
        for line in handle:
            if line.startswith(key + "="):
                return line.split("=", 1)[1].strip().strip('"')
    return None


def union_length(intervals, end):
    total, covered = 0.0, 0.0
    for start, stop in sorted(intervals):
        start, stop = max(start, covered), min(stop, end)
        if stop > start:
            total += stop - start
            covered = stop
    return total


def analyse(run_dir, horizon):
    verification = json.load(open(os.path.join(run_dir, "verification.json")))
    faults = [f["atSecond"] for f in verification.get("faultRecovery") or []]
    duration = float(env_value(os.path.join(run_dir, "scenario.env"), "DURATION_SECONDS"))
    impact_seconds = union_length([(f, f + horizon) for f in faults], duration)
    late_in = {k: 0 for k in KINDS}
    late_out = {k: 0 for k in KINDS}
    over_2s = over_10s = aggregation_only = verifier_check = 0
    with gzip.open(os.path.join(run_dir, "latencies.csv.gz"), "rt") as handle:
        next(handle)
        for line in handle:
            kind, _, append_s, ready_ms, done_ms, _ = line.split(",")
            append_s = float(append_s)
            if append_s < 0 or append_s >= duration:
                continue
            done_ms = float(done_ms)
            late = done_ms > SLO_MS
            if late and any(f <= append_s < f + VERIFIER_WINDOW_SECONDS for f in faults):
                verifier_check += 1
            done_s = append_s + done_ms / 1000
            if not any(append_s < f + horizon and done_s >= f for f in faults):
                late_out[kind] += late
                continue
            late_in[kind] += late
            if kind == "INDIVIDUAL":
                over_2s += done_ms > 2000
                over_10s += done_ms > 10000
            elif kind == "FINAL":
                aggregation_only += float(ready_ms) > SLO_MS
    reported = verification["latency"]["failure"].get("ALL", {}).get("overSlo", 0) if faults else 0
    if verifier_check != reported:
        print(f"{os.path.basename(os.path.normpath(run_dir))}: recomputed failure-window over-SLO {verifier_check}, "
              f"verification.json {reported}", file=sys.stderr)
    baseline_seconds = duration - impact_seconds
    expected = {k: late_out[k] / baseline_seconds * impact_seconds for k in KINDS}
    events = max(len(faults), 1)
    row = {
        "run": os.path.basename(os.path.normpath(run_dir)),
        "scenario": verification["scenario"],
        "variant": verification["variant"],
        "repetition": verification["repetition"],
        "faults": len(faults),
        "impact_s": impact_seconds,
        "baseline_late_events_per_min": late_out["INDIVIDUAL"] / baseline_seconds * 60,
        "late_events": late_in["INDIVIDUAL"],
        "baseline_events": expected["INDIVIDUAL"],
        "excess_events": late_in["INDIVIDUAL"] - expected["INDIVIDUAL"],
        "excess_aggregates": late_in["FINAL"] - expected["FINAL"],
        "excess_actions": sum(late_in.values()) - sum(expected.values()),
        "events_over_2s": over_2s,
        "events_over_10s": over_10s,
        "aggregation_only_late_aggregates": aggregation_only,
    }
    for column in COLUMNS:
        row[column] /= events
    return row


def fmt(value):
    return f"{value:,.0f}"


def summary(rows):
    groups = {}
    for row in rows:
        if row["faults"]:
            groups.setdefault((row["scenario"], row["variant"]), []).append(row)
    print("| Scenario | Variant | n | " + " | ".join(c.replace("_", " ") + " per event" for c in COLUMNS) + " |")
    print("|---" * (3 + len(COLUMNS)) + "|")
    for (scenario, variant), members in sorted(groups.items()):
        cells = []
        for column in COLUMNS:
            values = [m[column] for m in members]
            median = statistics.median(values)
            cells.append(fmt(median) if len(values) == 1 else f"{fmt(median)} [{fmt(min(values))}–{fmt(max(values))}]")
        print(f"| {scenario} | {variant.upper()} | {len(members)} | " + " | ".join(cells) + " |")


def write_run_file(run_dir, row, horizon):
    """Saves the per-disruption figures next to verification.json, so reports work without latencies.csv.gz."""
    data = {
        "source": "scripts/analysis/disruption_impact.py",
        "horizonSeconds": horizon,
        "sloMs": SLO_MS,
        "faults": row["faults"],
        "impactSeconds": round(row["impact_s"], 1),
        "baselineLateEventsPerMinute": round(row["baseline_late_events_per_min"], 1),
        "perDisruption": {
            "lateEvents": round(row["late_events"], 1),
            "baselineLateEvents": round(row["baseline_events"], 1),
            "addedLateEvents": round(row["excess_events"], 1),
            "addedLateAggregates": round(row["excess_aggregates"], 1),
            "addedLateActions": round(row["excess_actions"], 1),
            "eventsOver2s": round(row["events_over_2s"], 1),
            "eventsOver10s": round(row["events_over_10s"], 1),
            "aggregationOnlyLateAggregates": round(row["aggregation_only_late_aggregates"], 1),
        },
    }
    with open(os.path.join(run_dir, "disruption.json"), "w") as handle:
        json.dump(data, handle, indent=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--horizon", type=float, default=120.0, help="seconds after each fault counted as its impact")
    parser.add_argument("--summary", action="store_true", help="median [min-max] per scenario and variant")
    parser.add_argument("--write", action="store_true", help="write <run>/disruption.json instead of printing")
    parser.add_argument("runs", nargs="+")
    args = parser.parse_args()
    runs = [r for r in args.runs if os.path.exists(os.path.join(r, "latencies.csv.gz"))]
    for missing in sorted(set(args.runs) - set(runs)):
        print(f"{os.path.basename(os.path.normpath(missing))}: latencies.csv.gz missing", file=sys.stderr)
    with multiprocessing.Pool() as pool:
        rows = pool.starmap(analyse, [(r, args.horizon) for r in runs])
    if args.write:
        for run_dir, row in zip(runs, rows):
            write_run_file(run_dir, row, args.horizon)
        return
    if args.summary:
        summary(rows)
        return
    print(",".join(rows[0].keys()) if rows else "")
    for row in rows:
        print(",".join(f"{v:.1f}" if isinstance(v, float) else str(v) for v in row.values()))


if __name__ == "__main__":
    main()
