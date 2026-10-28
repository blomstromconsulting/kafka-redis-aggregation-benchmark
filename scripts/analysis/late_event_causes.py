#!/usr/bin/env python3
"""Where the late events of a variant B fault run lost their time, by pod and stage.

Every INDIVIDUAL action (one per incoming event) whose end-to-end latency exceeded the SLO and whose event was
appended in [from, to) is classified by the pod that picked it up (the owner of its ingress partition at pickup time,
from the PARTITIONS_ASSIGNED / PARTITIONS_REVOKED timeline events) and by where the time went:

  handover        appended before that pod was assigned the partition: it waited while the partition moved
  before pickup   otherwise late before executor pickup (append -> pickup > SLO): consumption or merge was slow
  after pickup    picked up within the SLO, late in the executor -> persistence path

A pod counts as new when it started after the measurement began (INSTANCE_STARTING > 0). For new pods, the late
events outside handovers are also counted by pod age at pickup. Variant A has no per-partition ownership events
(its tasks move inside Kafka Streams), so A runs are skipped.

Usage: scripts/analysis/late_event_causes.py [--from 60] [--to 120] results/runs/<run> [...]
"""
import argparse
import collections
import csv
import gzip
import json
import os

SLO_MS = 500.0


def ownership(run_dir):
    changes = collections.defaultdict(list)  # partition -> [(second, owner or None)]
    started = {}
    with open(os.path.join(run_dir, "timeline.csv")) as handle:
        for row in csv.DictReader(handle):
            second, source, kind = float(row["second"]), row["source"], row["type"]
            if kind == "INSTANCE_STARTING":
                started[source] = second
            if kind in ("PARTITIONS_ASSIGNED", "PARTITIONS_REVOKED"):
                detail = json.loads(row["detail"])
                if detail.get("loop") == "ingress":
                    for partition in detail["partitions"]:
                        changes[partition].append((second, source if kind == "PARTITIONS_ASSIGNED" else None))
    return changes, started


def owner_at(changes, partition, second):
    owner, since = None, None
    for when, who in changes[partition]:
        if when <= second:
            owner, since = who, when
    return owner, since


def analyse(run_dir, lo, hi):
    changes, started = ownership(run_dir)
    if not changes:
        return None
    counts = collections.Counter()
    by_age = collections.Counter()
    with gzip.open(os.path.join(run_dir, "latencies.csv.gz"), "rt") as handle:
        column = {name: i for i, name in enumerate(next(handle).rstrip("\n").split(","))}
        for line in handle:
            fields = line.rstrip("\n").split(",")
            kind, partition, append_s, ready_ms, done_ms = (fields[column[name]] for name in
                                                            ("kind", "partition", "append_s", "ready_ms", "done_ms"))
            if kind != "INDIVIDUAL" or float(done_ms) <= SLO_MS:
                continue
            append_s = float(append_s)
            if append_s < lo or append_s >= hi:
                continue
            ready_ms = float(ready_ms)
            pickup_s = append_s + ready_ms / 1000
            owner, since = owner_at(changes, int(partition), pickup_s)
            new = owner is not None and started.get(owner, -1) > 0
            if since is not None and append_s < since:
                counts["handover"] += 1
                continue
            stage = "before pickup" if ready_ms > SLO_MS else "after pickup"
            counts[f"{stage}, {'new' if new else 'old'} pod"] += 1
            if new:
                by_age[int((pickup_s - started[owner]) // 5) * 5] += 1
    return counts, by_age


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--from", dest="lo", type=float, default=60.0, help="append second (default: 60, the fault)")
    parser.add_argument("--to", dest="hi", type=float, default=120.0, help="append second, exclusive (default: 120)")
    parser.add_argument("runs", nargs="+")
    args = parser.parse_args()
    for run_dir in args.runs:
        name = os.path.basename(os.path.normpath(run_dir))
        result = analyse(run_dir, args.lo, args.hi)
        if result is None:
            print(f"{name}: no ingress partition ownership events (variant A?), skipped")
            continue
        counts, by_age = result
        total = sum(counts.values())
        print(f"{name}: {total} late events appended in [{args.lo:g}, {args.hi:g}) s")
        for key, n in counts.most_common():
            print(f"  {key:24s} {n:7d}  {100 * n / total:5.1f} %")
        if by_age:
            print("  new pods, by pod age at pickup: "
                  + ", ".join(f"{age}-{age + 5} s: {n}" for age, n in sorted(by_age.items())))


if __name__ == "__main__":
    main()
