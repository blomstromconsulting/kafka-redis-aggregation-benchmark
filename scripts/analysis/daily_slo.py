#!/usr/bin/env python3
"""p99.95 over a whole day of traffic with a number of disruptions, built from steady and disruption runs.

A run measures 300 s; an SLO is usually evaluated over a day or a month. This script builds one day from measured
runs of the same variant and build:

  actions over L in the day = steady actions over L per second x (day - disruptions x impact period)
                              + disruptions x actions over L in one disruption's impact set

The steady rate comes from a steady run's measurement window. The impact set is every action in flight during
[fault, fault + horizon) (the overlap rule of disruption_impact.py); a run with several faults (S5) counts as that
many disruptions. The day's p99.95 is the latency at which the count reaches 0.05 % of the day's actions (1 ms
resolution). Every steady run is paired with every disruption run of the same variant from the same results
directory; the summary shows the median and range over those pairings.

Events are fragments (their INDIVIDUAL action), aggregates are FINAL actions, on two bases: result ready
(result_ms) and end to end (done_ms). The load is the runs' own (6,000 events/s, of which the workload leaves a
few fragments out or sends them as duplicates), so only the length of the day and the number of disruptions can be
changed. Steady state is extrapolated from 300 s runs: where steady runs of one variant disagree, so do the days.

Usage: scripts/analysis/daily_slo.py [--hours 12] [--per-day 1] [--horizon 120] [--pairs] \\
           results/runs/steady-* results/runs/rolling-*
  --pairs prints every pairing as CSV instead of the summary.
"""
import argparse
import collections
import gzip
import json
import math
import multiprocessing
import os
import statistics
import sys

QUANTILE = 0.9995
SLO_MS = 500
KINDS = {"INDIVIDUAL": "events", "FINAL": "aggregates"}
BASES = {"result_ms": "result ready", "done_ms": "end to end"}


def env_value(path, key):
    with open(path) as handle:
        for line in handle:
            if line.startswith(key + "="):
                return line.split("=", 1)[1].strip().strip('"')
    return None


def load(run_dir, horizon):
    """Latency histogram (1 ms buckets, rounded up) per basis and kind: whole window, or the impact set."""
    verification = json.load(open(os.path.join(run_dir, "verification.json")))
    faults = [f["atSecond"] for f in verification.get("faultRecovery") or []]
    duration = float(env_value(os.path.join(run_dir, "scenario.env"), "DURATION_SECONDS"))
    histograms = {(basis, kind): collections.Counter() for basis in BASES for kind in KINDS}
    counts = collections.Counter()
    with gzip.open(os.path.join(run_dir, "latencies.csv.gz"), "rt") as handle:
        column = {name: i for i, name in enumerate(next(handle).rstrip("\n").split(","))}
        i_kind, i_append = column["kind"], column["append_s"]
        indexes = [(basis, column[basis]) for basis in BASES]
        for line in handle:
            fields = line.rstrip("\n").split(",")
            kind, append_s = fields[i_kind], float(fields[i_append])
            if kind not in KINDS or append_s < 0 or append_s >= duration:
                continue
            counts[kind] += 1
            for basis, index in indexes:
                latency_ms = float(fields[index])
                if faults and not any(append_s < f + horizon and append_s + latency_ms / 1000 >= f for f in faults):
                    continue
                histograms[(basis, kind)][math.ceil(latency_ms)] += 1
    impact_seconds = 0.0
    covered = 0.0
    for start, stop in sorted((f, min(f + horizon, duration)) for f in faults):
        start = max(start, covered)
        if stop > start:
            impact_seconds += stop - start
            covered = stop
    return {
        "run": os.path.basename(os.path.normpath(run_dir)),
        "results": os.path.dirname(os.path.normpath(run_dir)),
        "variant": verification["variant"],
        "load_achieved": verification.get("load", {}).get("loadAchieved", True),
        "duration": duration,
        "faults": len(faults),
        "impact_s": impact_seconds,
        "counts": dict(counts),
        "histograms": {f"{basis}|{kind}": dict(h) for (basis, kind), h in histograms.items()},
    }


def build_day(steady, disruption, basis, kind, hours, per_day):
    """The day's p99.95 and late counts for one pairing of a steady run with a disruption run."""
    day_seconds = hours * 3600
    disruption_weight = per_day / disruption["faults"]
    steady_seconds = day_seconds - per_day * disruption["impact_s"] / disruption["faults"]
    steady_weight = steady_seconds / steady["duration"]
    actions = steady["counts"][kind] / steady["duration"] * day_seconds
    allowed = (1 - QUANTILE) * actions
    key = f"{basis}|{kind}"
    weights = collections.Counter()
    for bucket, n in steady["histograms"][key].items():
        weights[int(bucket)] += n * steady_weight
    for bucket, n in disruption["histograms"][key].items():
        weights[int(bucket)] += n * disruption_weight
    p9995, over = None, 0.0
    for bucket in sorted(weights, reverse=True):
        over += weights[bucket]
        if over > allowed:
            p9995 = bucket
            break
    late_steady = sum(n for b, n in steady["histograms"][key].items() if int(b) > SLO_MS) * steady_weight
    late_disruption = sum(n for b, n in disruption["histograms"][key].items() if int(b) > SLO_MS) * disruption_weight
    return {"p9995_ms": p9995, "late": late_steady + late_disruption, "late_from_disruptions": late_disruption,
            "allowed": allowed, "actions": actions}


def pairings(runs, hours, per_day):
    groups = collections.defaultdict(lambda: {"steady": [], "disruption": []})
    for run in runs:
        groups[(run["results"], run["variant"])]["disruption" if run["faults"] else "steady"].append(run)
    for (results, variant), group in sorted(groups.items()):
        for steady in group["steady"]:
            for disruption in group["disruption"]:
                for basis in BASES:
                    for kind in KINDS:
                        yield {"results": results, "variant": variant, "steady": steady["run"],
                               "disruption": disruption["run"], "basis": basis, "kind": KINDS[kind],
                               **build_day(steady, disruption, basis, kind, hours, per_day)}


def fmt(values, digits=0):
    median = statistics.median(values)
    text = lambda v: f"{v:,.{digits}f}"
    return text(median) if len(values) == 1 else f"{text(median)} [{text(min(values))}–{text(max(values))}]"


def summary(days, hours, per_day):
    groups = collections.defaultdict(list)
    for day in days:
        groups[(day["results"], day["variant"], day["basis"], day["kind"])].append(day)
    print(f"Day: {hours:g} h of traffic, {per_day:g} disruption(s); median [min–max] over steady x disruption "
          f"pairings.\n")
    print("| Results | Variant | Basis | Kind | Pairings | p99.95 of the day, ms | Days at or under 500 ms "
          "| Over 500 ms per day | Allowed | Of which from the disruptions |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for (results, variant, basis, kind), members in sorted(groups.items()):
        p = [m["p9995_ms"] for m in members]
        passing = sum(1 for v in p if v <= SLO_MS)
        print(f"| {results} | {variant.upper()} | {BASES[basis]} | {kind} | {len(members)} | {fmt(p)} "
              f"| {passing} of {len(members)} | {fmt([m['late'] for m in members])} "
              f"| {fmt([m['allowed'] for m in members])} | {fmt([m['late_from_disruptions'] for m in members])} |")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--hours", type=float, default=12.0, help="hours of traffic in the day")
    parser.add_argument("--per-day", type=float, default=1.0, help="disruptions in the day")
    parser.add_argument("--horizon", type=float, default=120.0, help="seconds after each fault counted as its impact")
    parser.add_argument("--pairs", action="store_true", help="print every pairing as CSV")
    parser.add_argument("runs", nargs="+")
    args = parser.parse_args()
    runs = [r for r in args.runs if os.path.exists(os.path.join(r, "latencies.csv.gz"))]
    for missing in sorted(set(args.runs) - set(runs)):
        print(f"{os.path.basename(os.path.normpath(missing))}: latencies.csv.gz missing", file=sys.stderr)
    with multiprocessing.Pool() as pool:
        loaded = pool.starmap(load, [(r, args.horizon) for r in runs])
    for run in loaded:
        if not run["load_achieved"]:
            print(f"{run['run']}: offered load not achieved (verification.json), included", file=sys.stderr)
    days = list(pairings(loaded, args.hours, args.per_day))
    if args.pairs:
        print(",".join(days[0].keys()) if days else "")
        for day in days:
            print(",".join(f"{v:.1f}" if isinstance(v, float) else str(v) for v in day.values()))
        return
    summary(days, args.hours, args.per_day)


if __name__ == "__main__":
    main()
