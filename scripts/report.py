#!/usr/bin/env python3
"""Builds the comparative benchmark report from saved run results (stdlib only).

Inputs : results/runs/<run>/verification.json, resources.json, per_second_latency.csv, lag.csv,
         throughput.csv, timeline.csv; results/not-run/not-run.csv
Outputs: results/summary.csv, results/summary.json, results/report.html, docs/RESULTS.md

Every number in the outputs is read from those files. Repetitions are shown individually and summarised as
median [min-max] (n). Nothing is extrapolated; missing scenario/variant combinations are reported as NOT RUN.
"""
import csv
import html
import json
import math
import os
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.environ.get("AGGBENCH_RESULTS", os.path.join(ROOT, "results"))
VARIANTS = ["a1", "a2", "b"]
VARIANT_NAMES = {"a1": "A1 Streams (streams protocol)", "a2": "A2 Streams (classic protocol)", "b": "B Consumers + Redis"}
SHORT = {"a1": "A1 Streams", "a2": "A2 Classic", "b": "B Redis"}
# Series styling is done with CSS classes bound to theme tokens (see the report stylesheet).
COLORS = {"a1": "a1", "a2": "a2", "b": "b"}
PATHS = ["INDIVIDUAL", "STAGE2", "FINAL"]
SCENARIO_ORDER = ["smoke", "medium", "steady", "stress", "rolling", "abrupt-pod", "abrupt-container", "cold-pod",
                  "rescheduling", "state-1s", "state-10s", "state-60s", "state-300s", "slow-ml", "redis-restart",
                  "redis-loss", "redis-failover", "correctness", "kafka-restart"]
SATURATION_THRESHOLD = 0.90


# ----------------------------------------------------------------------------------------------- loading

def load_runs():
    runs = []
    base = os.path.join(RESULTS, "runs")
    if not os.path.isdir(base):
        return runs
    for name in sorted(os.listdir(base)):
        path = os.path.join(base, name)
        vfile = os.path.join(path, "verification.json")
        if not os.path.isfile(vfile):
            continue
        with open(vfile) as f:
            v = json.load(f)
        run = {"id": name, "dir": path, "v": v, "scenario": v.get("scenario"), "variant": v.get("variant"),
               "rep": v.get("repetition")}
        rfile = os.path.join(path, "resources.json")
        run["resources"] = json.load(open(rfile)) if os.path.isfile(rfile) else {}
        runs.append(run)
    return runs


def read_csv(path):
    if not os.path.isfile(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def not_run_entries():
    path = os.path.join(RESULTS, "not-run", "not-run.csv")
    entries = set()
    if os.path.isfile(path):
        for line in open(path):
            parts = line.strip().split(",", 2)
            if len(parts) == 3:
                entries.add((parts[0], parts[1], parts[2]))
    return entries


# ----------------------------------------------------------------------------------------------- metrics

def g(d, *keys, default=None):
    for k in keys:
        if not isinstance(d, dict) or k not in d:
            return default
        d = d[k]
    return d


def worst_path(latency_block, metric="p99_95"):
    """Highest value of the metric over the three end-to-end paths (the SLO applies to all of them)."""
    values = [g(latency_block, p, metric) for p in PATHS]
    values = [x for x in values if x is not None]
    return max(values) if values else None


def saturated(run):
    util = g(run["resources"], "nodeCpuUtilization", "p95")
    return util is not None and util > SATURATION_THRESHOLD


def run_metrics(run):
    v = run["v"]
    lat = v.get("latency", {})
    faults = v.get("faultRecovery", []) or []
    groups = g(run["resources"], "groups", default={}) or {}
    main_lag = None
    for group, data in (v.get("lag") or {}).items():
        if not group.endswith("-intents"):
            main_lag = data
    recovery_s = [f.get("latencyRecoverySeconds") for f in faults]
    # Longest gap between durable completions on one ingress partition after a fault (only gaps > 2 s are listed
    # by the verifier, so 0 means "no partition stalled for more than 2 s").
    stalls = [p.get("stallMs", 0) for f in faults for p in (f.get("stalledPartitions") or [])]
    numeric_recovery = [x for x in recovery_s if isinstance(x, (int, float))]
    m = {
        "run": run["id"], "scenario": run["scenario"], "variant": run["variant"], "rep": run["rep"],
        "load_achieved": g(v, "load", "loadAchieved"),
        "requested_rate": g(v, "load", "requestedRate"),
        "achieved_rate": g(v, "load", "achievedRateMeasure"),
        "processed_per_s": g(v, "throughput", "individualActionsPerSecondMedian"),
        "correctness": g(v, "correctness", "status"),
        "correctness_failures": len(g(v, "correctness", "failures", default=[]) or []),
        "steady_p9995": worst_path(lat.get("steady", {})),
        "measure_p9995": worst_path(lat.get("measure", {})),
        "failure_p9995": worst_path(lat.get("failure", {})) if lat.get("failure") else None,
        "final_steady_p9995": g(lat, "steady", "FINAL", "p99_95"),
        "final_measure_p99": g(lat, "measure", "FINAL", "p99"),
        "individual_measure_p9995": g(lat, "measure", "INDIVIDUAL", "p99_95"),
        "stage2_measure_p9995": g(lat, "measure", "STAGE2", "p99_95"),
        "final_measure_p9995": g(lat, "measure", "FINAL", "p99_95"),
        "measure_max": worst_path(lat.get("measure", {}), "max"),
        "over_slo": g(lat, "measure", "ALL", "overSlo"),
        "aggregation_only_p9995": worst_path(lat.get("aggregationOnly", {})),
        "aggregation_only_steady_p9995": worst_path(lat.get("aggregationOnlySteady", {})),
        "first_to_final_p9995": g(lat, "firstFragmentToFinal", "FINAL", "p99_95"),
        "max_lag": g(main_lag or {}, "maxLag"),
        "max_oldest_age_ms": g(main_lag or {}, "maxOldestUnprocessedAgeMs"),
        "recovery_s": max(numeric_recovery) if numeric_recovery else (None if not faults else "not recovered"),
        "max_partition_stall_ms": (max(stalls) if stalls else 0) if faults else None,
        "stalled_partitions": len({p.get("partition") for f in faults for p in (f.get("stalledPartitions") or [])}) if faults else None,
        "last_stall_end_ms": max([f.get("maxPartitionResumeMs", 0) for f in faults], default=None),
        "restored_records": g(v, "events", "RESTORE_END", "recordsRestored"),
        "max_restore_ms": g(v, "events", "RESTORE_END", "maxDurationMs"),
        "aggregator_cpu": g(groups, "aggregator", "cpuCoresAvg"),
        "aggregator_mem_mb": (g(groups, "aggregator", "memoryMaxBytes") or 0) / 2**20 or None,
        "redis_cpu": g(groups, "redis", "cpuCoresAvg"),
        "redis_mem_mb": (g(groups, "redis", "memoryMaxBytes") or 0) / 2**20 or None,
        "kafka_cpu": g(groups, "kafka", "cpuCoresAvg"),
        "kafka_mem_mb": (g(groups, "kafka", "memoryMaxBytes") or 0) / 2**20 or None,
        "node_cpu_p95": g(run["resources"], "nodeCpuUtilization", "p95"),
        "saturated": saturated(run),
        "slo_overall": g(v, "slo", "overall"),
        "slo_steady": g(v, "slo", "steadyStateLatency"),
        "slo_failure": g(v, "slo", "failureWindowLatency"),
    }
    if m["saturated"] and m["slo_overall"] in ("PASS", "FAIL") and m["correctness"] != "FAIL":
        m["slo_overall"] = "INCONCLUSIVE"
        m["slo_note"] = f"node CPU p95 {m['node_cpu_p95']:.0%} > {SATURATION_THRESHOLD:.0%} (resource-saturated)"
    return m


def summarize(values):
    nums = [x for x in values if isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x))]
    if not nums:
        return None
    return {"median": statistics.median(nums), "min": min(nums), "max": max(nums), "n": len(nums)}


def fmt(value, digits=0, unit=""):
    if value is None:
        return "—"
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "yes" if value else "no"
    if digits == 0:
        return f"{value:,.0f}{unit}"
    return f"{value:,.{digits}f}{unit}"


def fmt_summary(s, digits=0, unit=""):
    if s is None:
        return "NOT RUN"
    if s["n"] == 1:
        return f"{fmt(s['median'], digits, unit)} (n=1)"
    return f"{fmt(s['median'], digits, unit)} [{fmt(s['min'], digits)}–{fmt(s['max'], digits)}] (n={s['n']})"


def combined_status(statuses):
    statuses = [s for s in statuses if s]
    if not statuses:
        return "NOT RUN"
    if any(s == "FAIL" for s in statuses):
        return "FAIL"
    if any(s == "INCONCLUSIVE" for s in statuses):
        return "INCONCLUSIVE"
    if all(s in ("PASS", "NOT_APPLICABLE") for s in statuses):
        return "PASS" if any(s == "PASS" for s in statuses) else "NOT_APPLICABLE"
    return "INCONCLUSIVE"


# ----------------------------------------------------------------------------------------------- SVG charts

def svg_line_chart(title, series, x_label, y_label, markers=(), width=860, height=280, log_y=False, y_max=None):
    """series: list of (label, color, [(x, y), ...], dashed)."""
    pts = [(x, y) for _, _, data, _ in series for x, y in data if y is not None and (not log_y or y > 0)]
    if not pts:
        return f"<p class='muted'>{html.escape(title)}: no data</p>"
    left, right, top, bottom = 64, 16, 28, 40
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    x0, x1 = min(xs), max(xs)
    if x1 == x0:
        x1 = x0 + 1
    if log_y:
        y0, y1 = math.log10(max(min(ys), 0.1)), math.log10(max(ys) * 1.1)
    else:
        y0, y1 = 0, (y_max or max(ys) * 1.08 or 1)
    if y1 == y0:
        y1 = y0 + 1

    def sx(x):
        return left + (x - x0) / (x1 - x0) * (width - left - right)

    def sy(y):
        val = math.log10(max(y, 0.1)) if log_y else y
        return height - bottom - (val - y0) / (y1 - y0) * (height - top - bottom)

    out = [f"<svg viewBox='0 0 {width} {height}' class='chart' role='img' aria-label='{html.escape(title)}'>",
           f"<text x='{left}' y='16' class='ct'>{html.escape(title)}</text>"]
    for i in range(5):
        frac = i / 4
        yv = y0 + frac * (y1 - y0)
        label = 10 ** yv if log_y else yv
        y = height - bottom - frac * (height - top - bottom)
        out.append(f"<line x1='{left}' x2='{width - right}' y1='{y:.1f}' y2='{y:.1f}' class='grid'/>")
        out.append(f"<text x='{left - 6}' y='{y + 4:.1f}' class='ax' text-anchor='end'>{label:,.0f}</text>")
    for i in range(6):
        xv = x0 + i / 5 * (x1 - x0)
        out.append(f"<text x='{sx(xv):.1f}' y='{height - bottom + 16}' class='ax' text-anchor='middle'>{xv:,.0f}</text>")
    out.append(f"<text x='{(left + width) / 2}' y='{height - 6}' class='ax' text-anchor='middle'>{html.escape(x_label)}</text>")
    out.append(f"<text x='14' y='{(top + height - bottom) / 2}' class='ax' text-anchor='middle' transform='rotate(-90 14 {(top + height - bottom) / 2})'>{html.escape(y_label)}</text>")
    for mx, mlabel in markers:
        if x0 <= mx <= x1:
            out.append(f"<line x1='{sx(mx):.1f}' x2='{sx(mx):.1f}' y1='{top}' y2='{height - bottom}' class='marker'><title>{html.escape(mlabel)}</title></line>")
    legend_x = left + 8
    for label, color, data, dashed in series:
        data = [(x, y) for x, y in data if y is not None and (not log_y or y > 0)]
        if not data:
            continue
        path = " ".join(f"{'M' if i == 0 else 'L'}{sx(x):.1f},{sy(y):.1f}" for i, (x, y) in enumerate(data))
        dash = " stroke-dasharray='4 3'" if dashed else ""
        out.append(f"<path d='{path}' class='ln s-{color}'{dash}/>")
        out.append(f"<rect x='{legend_x}' y='{top + 2}' width='10' height='3' class='f-{color}'/>"
                   f"<text x='{legend_x + 14}' y='{top + 7}' class='lg'>{html.escape(label)}</text>")
        legend_x += 14 + 7 * len(label) + 16
    out.append("</svg>")
    return "".join(out)


def svg_bar_chart(title, categories, groups, unit="", width=860, height=260):
    """categories: x labels; groups: list of (label, color, [value per category])."""
    values = [v for _, _, vals in groups for v in vals if isinstance(v, (int, float))]
    if not values:
        return f"<p class='muted'>{html.escape(title)}: no data</p>"
    left, right, top, bottom = 64, 16, 40, 48
    vmax = max(values) * 1.15 or 1
    plot_w = width - left - right
    band = plot_w / max(1, len(categories))
    bar_w = band * 0.8 / max(1, len(groups))
    out = [f"<svg viewBox='0 0 {width} {height}' class='chart' role='img' aria-label='{html.escape(title)}'>",
           f"<text x='{left}' y='16' class='ct'>{html.escape(title)}</text>"]
    for i in range(5):
        frac = i / 4
        y = height - bottom - frac * (height - top - bottom)
        out.append(f"<line x1='{left}' x2='{width - right}' y1='{y:.1f}' y2='{y:.1f}' class='grid'/>")
        out.append(f"<text x='{left - 6}' y='{y + 4:.1f}' class='ax' text-anchor='end'>{vmax * frac:,.1f}</text>")
    for ci, cat in enumerate(categories):
        cx = left + ci * band + band / 2
        out.append(f"<text x='{cx:.1f}' y='{height - bottom + 16}' class='ax' text-anchor='middle'>{html.escape(cat)}</text>")
        for gi, (_, color, vals) in enumerate(groups):
            v = vals[ci] if ci < len(vals) else None
            if not isinstance(v, (int, float)):
                continue
            x = left + ci * band + band * 0.1 + gi * bar_w
            h = (v / vmax) * (height - top - bottom)
            out.append(f"<rect x='{x:.1f}' y='{height - bottom - h:.1f}' width='{bar_w * 0.92:.1f}' height='{h:.1f}' class='f-{color}'>"
                       f"<title>{v:,.2f}{unit}</title></rect>")
    legend_x = left + 8
    for label, color, _ in groups:
        out.append(f"<rect x='{legend_x}' y='24' width='10' height='8' class='f-{color}'/>"
                   f"<text x='{legend_x + 14}' y='32' class='lg'>{html.escape(label)}</text>")
        legend_x += 14 + 7 * len(label) + 16
    out.append("</svg>")
    return "".join(out)


def svg_timeline(title, rows, x_range, width=860):
    """rows: list of (row label, color, [(x, label)])."""
    if not rows:
        return f"<p class='muted'>{html.escape(title)}: no events</p>"
    left, right, top, row_h = 210, 16, 28, 18
    height = top + row_h * len(rows) + 30
    x0, x1 = x_range
    if x1 <= x0:
        x1 = x0 + 1

    def sx(x):
        return left + (x - x0) / (x1 - x0) * (width - left - right)

    out = [f"<svg viewBox='0 0 {width} {height}' class='chart' role='img' aria-label='{html.escape(title)}'>",
           f"<text x='8' y='16' class='ct'>{html.escape(title)}</text>"]
    for i, (label, color, events) in enumerate(rows):
        y = top + i * row_h + row_h / 2
        out.append(f"<text x='{left - 6}' y='{y + 4}' class='ax' text-anchor='end'>{html.escape(label)}</text>")
        out.append(f"<line x1='{left}' x2='{width - right}' y1='{y}' y2='{y}' class='grid'/>")
        for x, tip in events:
            if x0 <= x <= x1:
                out.append(f"<circle cx='{sx(x):.1f}' cy='{y}' r='3' class='f-{color}'><title>{html.escape(tip)}</title></circle>")
    for i in range(6):
        xv = x0 + i / 5 * (x1 - x0)
        out.append(f"<text x='{sx(xv):.1f}' y='{height - 10}' class='ax' text-anchor='middle'>{xv:,.0f}s</text>")
    out.append("</svg>")
    return "".join(out)


# ----------------------------------------------------------------------------------------------- report

def per_second_series(run, kind="ALL", column="p99_95_ms"):
    rows = read_csv(os.path.join(run["dir"], "per_second_latency.csv"))
    return [(float(r["second"]), float(r[column])) for r in rows if r["kind"] == kind]


def fault_markers(run):
    markers = []
    for r in read_csv(os.path.join(run["dir"], "timeline.csv")):
        if r["type"].startswith("FAULT_"):
            markers.append((float(r["second"]), r["type"]))
    return markers


def lag_series(run):
    rows = read_csv(os.path.join(run["dir"], "lag.csv"))
    return [(float(r["second"]), float(r["lag"])) for r in rows if not r["group"].endswith("-intents")]


def throughput_series(run):
    rows = read_csv(os.path.join(run["dir"], "throughput.csv"))
    return [(float(r["second"]), float(r["individual_actions_completed"])) for r in rows]


def build(runs):
    metrics = [run_metrics(r) for r in runs]
    by = defaultdict(list)
    for m in metrics:
        by[(m["scenario"], m["variant"])].append(m)
    scenarios = [s for s in SCENARIO_ORDER if any(k[0] == s for k in by)] + \
        sorted({k[0] for k in by} - set(SCENARIO_ORDER))
    return metrics, by, scenarios


def main_table_rows(by):
    def s(scenario, variant, key):
        return summarize([m[key] for m in by.get((scenario, variant), []) if m["correctness"] != "FAIL"])

    rows = [
        ("Sustained throughput, fragments/s processed (S1)", lambda v: fmt_summary(s("steady", v, "processed_per_s"))),
        ("Producer achieved rate, fragments/s (S1)", lambda v: fmt_summary(s("steady", v, "achieved_rate"))),
        ("Normal p99.95, worst path, ms (S1 steady window)", lambda v: fmt_summary(s("steady", v, "steady_p9995"))),
        ("Aggregation-only p99.95, completing fragment append → executor pickup, ms (S1 steady window)",
         lambda v: fmt_summary(s("steady", v, "aggregation_only_steady_p9995"))),
        ("Rolling upgrade p99.95, ms (S2 whole window)", lambda v: fmt_summary(s("rolling", v, "measure_p9995"))),
        ("Abrupt failure p99.95, ms (S3 whole window)", lambda v: fmt_summary(s("abrupt-pod", v, "measure_p9995"))),
        ("Cold replacement p99.95, ms (S4 whole window)", lambda v: fmt_summary(s("cold-pod", v, "measure_p9995"))),
        ("Frequent rescheduling p99.95, ms (S5 whole window)", lambda v: fmt_summary(s("rescheduling", v, "measure_p9995"))),
        ("Recovery to SLO after abrupt failure, s (S3)", lambda v: fmt_summary(s("abrupt-pod", v, "recovery_s"))),
        ("Longest single-partition processing stall, ms (S3 abrupt kill)", lambda v: fmt_summary(s("abrupt-pod", v, "max_partition_stall_ms"))),
        ("Longest single-partition processing stall, ms (S2 rolling)", lambda v: fmt_summary(s("rolling", v, "max_partition_stall_ms"))),
        ("Partitions stalled > 2 s (S5 rescheduling, distinct)", lambda v: fmt_summary(s("rescheduling", v, "stalled_partitions"))),
        ("Maximum backlog, records (S5)", lambda v: fmt_summary(s("rescheduling", v, "max_lag"))),
        ("Oldest unprocessed record, ms (S5)", lambda v: fmt_summary(s("rescheduling", v, "max_oldest_age_ms"))),
        ("Actions exceeding 500 ms (S5)", lambda v: fmt_summary(s("rescheduling", v, "over_slo"))),
        ("Correctness errors (all runs)", lambda v: correctness_cell(by, v)),
        ("Aggregator CPU, cores avg (S1, 3 pods)", lambda v: fmt_summary(s("steady", v, "aggregator_cpu"), 2)),
        ("Aggregator memory, MiB max (S1, 3 pods)", lambda v: fmt_summary(s("steady", v, "aggregator_mem_mb"))),
        ("Redis CPU, cores avg / memory MiB (S1)", lambda v: f"{fmt_summary(s('steady', v, 'redis_cpu'), 2)} / {fmt_summary(s('steady', v, 'redis_mem_mb'))}" if v == "b" else "n/a"),
        ("Kafka broker CPU, cores avg (S1)", lambda v: fmt_summary(s("steady", v, "kafka_cpu"), 2)),
    ]
    return [(label, [f(v) for v in VARIANTS]) for label, f in rows]


def correctness_cell(by, variant):
    runs = [m for (sc, v), ms in by.items() if v == variant for m in ms]
    if not runs:
        return "NOT RUN"
    failed = [m for m in runs if m["correctness"] == "FAIL"]
    inconclusive = [m for m in runs if m["correctness"] == "INCONCLUSIVE"]
    text = f"{len(failed)} of {len(runs)} runs failed"
    if inconclusive:
        text += f", {len(inconclusive)} inconclusive"
    return text


def slo_rows(by, scenarios):
    rows = []
    for scenario in scenarios:
        cells = []
        for v in VARIANTS:
            ms = by.get((scenario, v), [])
            if not ms:
                cells.append("NOT RUN")
                continue
            cells.append(" · ".join([
                f"overall {combined_status([m['slo_overall'] for m in ms])}",
                f"correctness {combined_status([m['correctness'] for m in ms])}",
                f"steady {combined_status([m['slo_steady'] for m in ms])}",
                f"failure-window {combined_status([m['slo_failure'] for m in ms])}",
            ]))
        rows.append((scenario, cells))
    return rows


def status_class(text):
    for key in ("FAIL", "INCONCLUSIVE", "PASS", "NOT RUN"):
        if text.startswith(f"overall {key}") or text == key:
            return key.lower().replace(" ", "-")
    return ""


def write_summary(metrics):
    keys = list(metrics[0].keys()) if metrics else []
    with open(os.path.join(RESULTS, "summary.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        for m in metrics:
            writer.writerow(m)
    with open(os.path.join(RESULTS, "summary.json"), "w") as f:
        json.dump(metrics, f, indent=2)


def pick_representative(ms):
    """The repetition with the median worst-path p99.95 (whole window) is plotted."""
    candidates = [m for m in ms if m["measure_p9995"] is not None]
    if not candidates:
        return None
    candidates.sort(key=lambda m: m["measure_p9995"])
    return candidates[len(candidates) // 2]


def md_section(text, start, end=None):
    """Lines of a generated Markdown document from the heading starting with `start` up to the heading `end`."""
    lines, inside = [], False
    for line in text.splitlines():
        if line.startswith(start):
            inside = True
        elif end and line.startswith(end):
            inside = False
        if inside:
            lines.append(line)
    return lines


def md_to_html(lines):
    """Minimal renderer for the Markdown that results_md() writes: headings, bullets, paragraphs, bold, code."""
    def inline(text):
        text = html.escape(text, quote=False)
        text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
        return re.sub(r"`(.+?)`", r"<code>\1</code>", text)

    out, in_list = [], False
    for line in lines:
        if line.startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{inline(line[2:])}</li>")
            continue
        if in_list:
            out.append("</ul>")
            in_list = False
        if line.startswith("### "):
            out.append(f"<h3>{inline(line[4:])}</h3>")
        elif line.startswith("## "):
            out.append(f"<h2>{inline(line[3:])}</h2>")
        elif line.strip():
            out.append(f"<p>{inline(line)}</p>")
    if in_list:
        out.append("</ul>")
    return "".join(out)


def html_report(metrics, by, scenarios, runs_by_id, not_run, results_text):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    # Utilitarian report: neutral blue-grey ground, one colour per variant, semantic status fills.
    css = """
    :root{--bg:#f7f8fa;--surface:#ffffff;--fg:#1b2230;--muted:#5d6676;--line:#d9dee6;--grid:#e8ecf1;
      --a1:#2463c7;--a2:#c46a00;--b:#12805a;--fault:#c23b3b;--pass:#e3f3ea;--fail:#fbe4e4;--inc:#fdf1d6;
      --display:"IBM Plex Sans",-apple-system,"Segoe UI",sans-serif;--mono:"IBM Plex Mono",ui-monospace,Menlo,monospace}
    @media (prefers-color-scheme: dark){:root:not([data-theme="light"]){--bg:#12151b;--surface:#1a1e26;--fg:#e4e8ef;
      --muted:#9aa3b2;--line:#2c3340;--grid:#232933;--a1:#6ea2ff;--a2:#f0a24a;--b:#43c992;--fault:#ff7b7b;
      --pass:#173826;--fail:#4a1f22;--inc:#463a16;color-scheme:dark}}
    :root[data-theme="dark"]{--bg:#12151b;--surface:#1a1e26;--fg:#e4e8ef;--muted:#9aa3b2;--line:#2c3340;--grid:#232933;
      --a1:#6ea2ff;--a2:#f0a24a;--b:#43c992;--fault:#ff7b7b;--pass:#173826;--fail:#4a1f22;--inc:#463a16;color-scheme:dark}
    body{font:14px/1.5 var(--display);color:var(--fg);background:var(--bg);margin:0}
    main{max-width:1120px;margin:0 auto;padding-block:20px 48px;padding-left:max(16px,env(safe-area-inset-left,0px));
      padding-right:max(16px,env(safe-area-inset-right,0px))}
    h1{font-size:26px;font-weight:600;margin:0 0 6px;text-wrap:balance}
    h2{font-size:18px;font-weight:600;margin:36px 0 8px;padding-bottom:4px;border-bottom:1px solid var(--line);text-wrap:balance}
    h3{font-size:14px;font-weight:600;margin:20px 0 6px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
    p,ul{max-width:80ch}li{margin:3px 0}.muted{color:var(--muted)}code{font-family:var(--mono);font-size:12px}
    table{border-collapse:collapse;width:100%;margin:8px 0;font-size:13px;background:var(--surface);font-variant-numeric:tabular-nums}
    td,th{border:1px solid var(--line);padding:5px 8px;vertical-align:top;text-align:left}
    th{background:var(--grid);font-weight:600}td:first-child{min-width:14ch}
    .pass{background:var(--pass)}.fail{background:var(--fail)}.inconclusive{background:var(--inc)}
    .wrap{overflow-x:auto;max-width:100%}.chart{width:100%;max-width:100%;height:auto;margin:8px 0;display:block}
    .chart .grid{stroke:var(--grid)}.chart .ax,.chart .lg{fill:var(--muted);font:10px var(--mono)}
    .chart .ct{fill:var(--fg);font:600 12px var(--display)}.chart .marker{stroke:var(--fault);stroke-dasharray:3 3}
    .chart .ln{fill:none;stroke-width:1.4}.s-a1{stroke:var(--a1)}.s-a2{stroke:var(--a2)}.s-b{stroke:var(--b)}
    .f-a1{fill:var(--a1)}.f-a2{fill:var(--a2)}.f-b{fill:var(--b)}
    .key{display:flex;flex-wrap:wrap;gap:6px 18px;margin:6px 0 0;font-size:13px}.key span::before{content:"";display:inline-block;
      width:10px;height:10px;margin-right:6px;border-radius:2px;vertical-align:-1px}
    .key .k-a1::before{background:var(--a1)}.key .k-a2::before{background:var(--a2)}.key .k-b::before{background:var(--b)}
    """
    # Written as a page fragment (title, meta, style, content): the artifact host adds the document skeleton,
    # and browsers opening the local file place these elements correctly as well.
    out = ["<meta charset='utf-8'>", "<title>Streams vs Redis Benchmark</title>",
           "<meta name='claude:viewport-fit-sides' content='cover'>",
           "<link rel='stylesheet' href='https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;600&display=swap'>",
           f"<style>{css}</style><main>",
           "<h1>Kafka Streams vs Redis aggregation benchmark</h1>",
           "<p class='key'><span class='k-a1'>A1 Kafka Streams, streams protocol</span><span class='k-a2'>A2 Kafka Streams, classic protocol</span>"
           "<span class='k-b'>B Kafka consumers + Redis</span></p>",
           f"<p class='muted'>Generated {now} from {len(metrics)} runs in <code>results/runs</code>. "
           "Values are median [min–max] (n repetitions); latency is end-to-end from the broker append time of the "
           "latest required fragment to the durable PostgreSQL commit, exact percentiles over all actions. "
           "“Worst path” is the highest of the INDIVIDUAL, STAGE2 and FINAL paths. Runs with correctness FAIL are "
           "excluded from performance summaries.</p>"]

    # Findings, recommendation and limitations are the RESULTS.md text, so the page and the document cannot disagree.
    out.append(md_to_html(md_section(results_text, "## Recommendation", "## Limitations")))

    out.append("<h2>Main comparison</h2><div class='wrap'><table><tr><th>Metric</th>" +
               "".join(f"<th>{SHORT[v]}</th>" for v in VARIANTS) + "</tr>")
    for label, cells in main_table_rows(by):
        out.append(f"<tr><td>{html.escape(label)}</td>" + "".join(f"<td>{html.escape(c)}</td>" for c in cells) + "</tr>")
    out.append("</table></div>")

    out.append("<h2>SLO evaluation (p99.95 ≤ 500 ms on every path)</h2><div class='wrap'><table><tr><th>Scenario</th>" +
               "".join(f"<th>{SHORT[v]}</th>" for v in VARIANTS) + "</tr>")
    for scenario, cells in slo_rows(by, scenarios):
        out.append(f"<tr><td>{html.escape(scenario)}</td>" + "".join(
            f"<td class='{status_class(c)}'>{html.escape(c)}</td>" for c in cells) + "</tr>")
    out.append("</table></div><p class='muted'>overall: PASS only if the offered load was achieved, correctness "
               "passed, and both steady-state and failure-window p99.95 met the SLO; INCONCLUSIVE when the load was "
               "not reached, the node was CPU-saturated (p95 &gt; 90%), a window had too few samples, or the run did "
               "not drain.</p>")
    if not_run:
        out.append("<h3>Not run</h3><ul>" + "".join(f"<li>{html.escape(s)} / {html.escape(v)}: {html.escape(r)}</li>"
                                                     for s, v, r in sorted(not_run)) + "</ul>")

    out.append(md_to_html(md_section(results_text, "## Findings", "## Recommendation")))

    for scenario in scenarios:
        reps = {v: pick_representative(by.get((scenario, v), [])) for v in VARIANTS}
        present = {v: runs_by_id[m["run"]] for v, m in reps.items() if m}
        if not present:
            continue
        out.append(f"<h2>Scenario: {html.escape(scenario)}</h2>")
        desc = next(iter(present.values()))["v"].get("config", {}).get("plan", {})
        out.append(f"<p class='muted'>rate {desc.get('eventRate')} fragments/s · correlation window {desc.get('correlationWindowMs')} ms · "
                   f"seed {desc.get('seed')} · plotted repetition = median run per variant · red dashed lines = injected faults</p>")
        markers = []
        for run in present.values():
            markers += fault_markers(run)
        out.append(svg_line_chart("p99.95 latency per second of arrival (all paths)",
                                  [(SHORT[v], COLORS[v], per_second_series(r), False) for v, r in present.items()],
                                  "seconds since measurement start", "ms (log)", markers, log_y=True))
        out.append(svg_line_chart("Unprocessed records (committed-offset lag of the aggregation group)",
                                  [(SHORT[v], COLORS[v], lag_series(r), False) for v, r in present.items()],
                                  "seconds since measurement start", "records", markers))
        out.append(svg_line_chart("Throughput: individual actions durably completed per second",
                                  [(SHORT[v], COLORS[v], throughput_series(r), False) for v, r in present.items()],
                                  "seconds since measurement start", "actions/s", markers))
        rows = []
        for v, run in present.items():
            events = defaultdict(list)
            for r in read_csv(os.path.join(run["dir"], "timeline.csv")):
                t = r["type"]
                if t in ("TASKS_CHANGED", "PARTITIONS_ASSIGNED", "PARTITIONS_REVOKED", "PARTITIONS_LOST", "RESTORE_START",
                         "RESTORE_END", "TASK_PROCESSING_STARTED", "PARTITION_PROCESSING_STARTED", "REDIS_EPOCH_CHANGED",
                         "REPLAY_STARTED", "STREAMS_STATE", "REPLACEMENT_READY") or t.startswith("FAULT_"):
                    events[t].append((float(r["second"]), f"{t} {r['source']} {r['detail'][:160]}"))
            for t in sorted(events):
                rows.append((f"{SHORT[v]}: {t}", COLORS[v], events[t]))
        xs = [x for _, _, ev in rows for x, _ in ev]
        if xs:
            out.append(svg_timeline("Assignment, restoration and recovery timeline", rows, (min(min(xs), 0), max(xs))))
        series = []
        for v, run in present.items():
            res = run["resources"].get("series", {})
            if v in ("a1", "a2"):
                series.append((f"{SHORT[v]} restored records/s", COLORS[v], [tuple(p) for p in res.get("streamsRestoredRecordsPerSecond", [])], False))
            else:
                series.append((f"{SHORT[v]} Redis merge p99 (ms)", COLORS[v], [(p[0], p[1] * 1000) for p in res.get("redisMergeP99Seconds", [])], True))
        out.append(svg_line_chart("Kafka Streams restoration rate and Redis merge latency", series, "seconds since measurement start", "records/s · ms", markers))

        out.append("<h3>Individual runs</h3><div class='wrap'><table><tr><th>Run</th><th>Load ok</th><th>Correctness</th>"
                   "<th>p99.95 steady (worst path)</th><th>p99.95 whole window</th><th>p99.95 failure window</th><th>max</th>"
                   "<th>&gt;500 ms</th><th>aggregation-only p99.95</th><th>first→final p99.95</th><th>max lag</th><th>recovery s</th><th>longest stall ms</th>"
                   "<th>restored records</th><th>node CPU p95</th><th>SLO</th></tr>")
        for v in VARIANTS:
            for m in sorted(by.get((scenario, v), []), key=lambda m: m["rep"] or 0):
                out.append("<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in [
                    m["run"], fmt(m["load_achieved"]), m["correctness"], fmt(m["steady_p9995"], 1), fmt(m["measure_p9995"], 1),
                    fmt(m["failure_p9995"], 1), fmt(m["measure_max"], 1), fmt(m["over_slo"]), fmt(m["aggregation_only_p9995"], 1),
                    fmt(m["first_to_final_p9995"], 1), fmt(m["max_lag"]), fmt(m["recovery_s"]), fmt(m["max_partition_stall_ms"]), fmt(m["restored_records"]),
                    fmt((m["node_cpu_p95"] or 0) * 100, 0, "%") if m["node_cpu_p95"] is not None else "—",
                    m["slo_overall"] + (f" ({m['slo_note']})" if m.get("slo_note") else "")]) + "</tr>")
        out.append("</table></div>")

    # Recovery and restoration comparisons across fault scenarios.
    fault_scenarios = [s for s in scenarios if any(m["recovery_s"] is not None for v in VARIANTS for m in by.get((s, v), []))]
    if fault_scenarios:
        out.append("<h2>Recovery after injected failures</h2>")
        groups = []
        for v in VARIANTS:
            vals = []
            for s in fault_scenarios:
                summ = summarize([m["max_partition_stall_ms"] for m in by.get((s, v), [])])
                vals.append(summ["median"] / 1000 if summ else None)
            groups.append((SHORT[v], COLORS[v], vals))
        out.append(svg_bar_chart("Longest single-partition processing stall in the fault windows (s, median of runs)", fault_scenarios, groups, " s"))
        groups = []
        for v in VARIANTS:
            vals = []
            for s in fault_scenarios:
                summ = summarize([m["failure_p9995"] for m in by.get((s, v), [])])
                vals.append(summ["median"] if summ else None)
            groups.append((SHORT[v], COLORS[v], vals))
        out.append(svg_bar_chart("Failure-window p99.95, worst path (ms, median of runs)", fault_scenarios, groups, " ms"))
    state_scenarios = [s for s in ["state-1s", "state-10s", "state-60s", "state-300s"] if any((s, v) in by for v in VARIANTS)]
    if state_scenarios:
        out.append("<h2>State size and restoration (S6)</h2>")
        groups = [(SHORT[v], COLORS[v], [(summarize([m["max_restore_ms"] for m in by.get((s, v), [])]) or {}).get("median") for s in state_scenarios])
                  for v in ("a1", "a2")]
        out.append(svg_bar_chart("Longest single changelog-partition restoration (ms)", state_scenarios, groups, " ms"))
        groups = [(SHORT[v], COLORS[v], [(summarize([m["restored_records"] for m in by.get((s, v), [])]) or {}).get("median") for s in state_scenarios])
                  for v in ("a1", "a2")]
        out.append(svg_bar_chart("Records restored from changelogs during the run", state_scenarios, groups))
        groups = [(SHORT[v], COLORS[v], [(summarize([m["failure_p9995"] for m in by.get((s, v), [])]) or {}).get("median") for s in state_scenarios])
                  for v in VARIANTS]
        out.append(svg_bar_chart("Failure-window p99.95 by correlation window (ms)", state_scenarios, groups, " ms"))
    steady = [s for s in ("steady", "medium", "smoke") if any((s, v) in by for v in VARIANTS)]
    if steady:
        s = steady[0]
        out.append(f"<h2>Resource consumption ({html.escape(s)})</h2>")
        cats = ["aggregator CPU", "Kafka CPU", "Redis CPU"]
        groups = [(SHORT[v], COLORS[v], [(summarize([m[k] for m in by.get((s, v), [])]) or {}).get("median")
                                         for k in ("aggregator_cpu", "kafka_cpu", "redis_cpu")]) for v in VARIANTS]
        out.append(svg_bar_chart("Average CPU over the measurement window (cores)", cats, groups, " cores"))
        cats = ["aggregator memory", "Kafka memory", "Redis memory"]
        groups = [(SHORT[v], COLORS[v], [(summarize([m[k] for m in by.get((s, v), [])]) or {}).get("median")
                                         for k in ("aggregator_mem_mb", "kafka_mem_mb", "redis_mem_mb")]) for v in VARIANTS]
        out.append(svg_bar_chart("Peak working-set memory (MiB)", cats, groups, " MiB"))
    out.append(md_to_html(md_section(results_text, "## Limitations")))
    out.append("<h2>Raw data</h2><p>Per run: <code>verification.json</code> (all statistics and correctness checks), "
               "<code>per_second_latency.csv</code>, <code>lag.csv</code>, <code>throughput.csv</code>, <code>producer.csv</code>, "
               "<code>timeline.csv</code>, <code>latencies.csv.gz</code> (every action), <code>resources.json</code>, logs and "
               "startup configuration reports. Summary: <code>results/summary.csv</code>.</p></main>")
    with open(os.path.join(RESULTS, "report.html"), "w") as f:
        f.write("\n".join(out))


# ----------------------------------------------------------------------------------------------- RESULTS.md

def results_md(metrics, by, scenarios, not_run):
    lines = ["# Results", "",
             f"_Generated by `scripts/report.sh` on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} from "
             f"{len(metrics)} runs. Do not edit by hand: every statement below is derived from the saved run data "
             "(`results/runs/*/verification.json`). See `results/report.html` for graphs and per-run tables._", ""]
    if not metrics:
        lines += ["**No benchmark runs have been executed yet.** All scenarios: NOT RUN.", ""]
        return "\n".join(lines)

    lines += ["## Main comparison", "", "| Metric | " + " | ".join(SHORT[v] for v in VARIANTS) + " |",
              "|---|" + "---|" * len(VARIANTS)]
    for label, cells in main_table_rows(by):
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    lines += ["", "Values: median [min–max] (n repetitions). “Worst path” = highest p99.95 among INDIVIDUAL, STAGE2 "
                  "and FINAL actions. Latency is end-to-end to the durable database commit.", ""]

    lines += ["## SLO evaluation", "", "| Scenario | " + " | ".join(SHORT[v] for v in VARIANTS) + " |", "|---|" + "---|" * 3]
    for scenario, cells in slo_rows(by, scenarios):
        lines.append(f"| {scenario} | " + " | ".join(cells) + " |")
    lines.append("")
    executed = {k for k in by}
    missing = [f"{s}/{v}" for s in SCENARIO_ORDER for v in VARIANTS
               if (s, v) not in executed and not (s.startswith("redis-") and v != "b")]
    if missing or not_run:
        lines += ["### Not run", "", "The following scenario/variant combinations have no results (NOT RUN): "
                  + ", ".join(missing) + ".", ""]
        for s, v, r in sorted(not_run):
            lines.append(f"- {s}/{v}: {r}")
        lines.append("")

    lines += ["## Findings (derived from the data above)", ""]
    clear_winners = {}  # (scenario, key) -> variant ahead with non-overlapping ranges and >= 10% difference, else None

    def best(scenario, key, label, lower_is_better=True):
        clear_winners[(scenario, key)] = None
        candidates = []
        for v in VARIANTS:
            ms = [m for m in by.get((scenario, v), []) if m["correctness"] == "PASS" and m["load_achieved"] and not m["saturated"]]
            summ = summarize([m[key] for m in ms])
            if summ:
                candidates.append((summ["median"], v, summ))
        if len(candidates) < 2:
            return f"- **{label}:** insufficient valid data (needs ≥ 2 variants with correctness PASS, achieved load and no CPU saturation; have {len(candidates)})."
        candidates.sort(reverse=not lower_is_better)
        (b_val, b_v, b_s), (r_val, r_v, r_s) = candidates[0], candidates[1]
        overlap = (b_s["max"] >= r_s["min"]) if lower_is_better else (b_s["min"] <= r_s["max"])
        rel = abs(b_val - r_val) / max(abs(r_val), 1e-9)
        if overlap or rel < 0.10:
            return (f"- **{label}:** no clear winner — {SHORT[b_v]} {fmt(b_val, 1)} vs {SHORT[r_v]} {fmt(r_val, 1)} "
                    f"(difference {rel:.0%}; repetition ranges {'overlap' if overlap else 'do not overlap'}).")
        clear_winners[(scenario, key)] = b_v
        return (f"- **{label}:** {SHORT[b_v]} ({fmt(b_val, 1)}) ahead of {SHORT[r_v]} ({fmt(r_val, 1)}), "
                f"difference {rel:.0%}, non-overlapping repetition ranges.")

    lines.append(best("steady", "steady_p9995", "Best steady-state latency (S1 p99.95 worst path, ms)"))
    lines.append(best("steady", "aggregation_only_steady_p9995",
                      "Lowest aggregation-only latency (S1 p99.95, append → executor pickup, ms)"))
    lines.append(best("steady", "processed_per_s", "Highest sustained throughput (S1, fragments/s)", lower_is_better=False))
    for scenario, label in (("rolling", "Lowest rolling-upgrade latency (S2 p99.95, ms)"),
                            ("abrupt-pod", "Lowest failure-induced latency, abrupt pod kill (S3 p99.95, ms)"),
                            ("cold-pod", "Lowest failure-induced latency, cold replacement (S4 p99.95, ms)"),
                            ("rescheduling", "Lowest latency under frequent rescheduling (S5 p99.95, ms)")):
        lines.append(best(scenario, "measure_p9995", label))
    lines.append(best("abrupt-pod", "max_partition_stall_ms", "Shortest partition stall after abrupt failure (S3, ms)"))
    lines.append(best("rolling", "max_partition_stall_ms", "Shortest partition stall during rolling upgrade (S2, ms)"))

    lines += ["", "### Recovery reliability", ""]
    for v in VARIANTS:
        ms = [m for (s, vv), lst in by.items() if vv == v for m in lst]
        if not ms:
            lines.append(f"- {SHORT[v]}: NOT RUN")
            continue
        fails = [m["run"] for m in ms if m["correctness"] == "FAIL"]
        unrec = [m["run"] for m in ms if m["recovery_s"] == "not recovered"]
        lines.append(f"- {SHORT[v]}: {len(ms)} runs, correctness failures: {len(fails)}"
                     + (f" ({', '.join(fails)})" if fails else "")
                     + f"; runs whose latency did not return under the SLO within the fault window: {len(unrec)}.")

    lines += ["", "### State size", ""]
    state = [s for s in ("state-1s", "state-10s", "state-60s", "state-300s") if any((s, v) in by for v in VARIANTS)]
    if not state:
        lines.append("- S6 (state size) NOT RUN.")
    else:
        for v in ("a1", "a2"):
            parts = []
            threshold = None
            for s in state:
                summ_restore = summarize([m["max_restore_ms"] for m in by.get((s, v), [])])
                summ_fail = summarize([m["failure_p9995"] for m in by.get((s, v), [])])
                parts.append(f"{s}: restore {fmt_summary(summ_restore)} ms, failure p99.95 {fmt_summary(summ_fail)} ms")
                if threshold is None and summ_fail and summ_fail["median"] > 500:
                    threshold = s
            lines.append(f"- {SHORT[v]}: " + "; ".join(parts) + ". "
                         + (f"First tested window where the failure-window p99.95 exceeds 500 ms: **{threshold}**."
                            if threshold else "No tested window pushed the failure-window p99.95 above 500 ms."))
        b_parts = [f"{s}: failure p99.95 {fmt_summary(summarize([m['failure_p9995'] for m in by.get((s, 'b'), [])]))} ms" for s in state]
        lines.append(f"- {SHORT['b']} (for comparison): " + "; ".join(b_parts) + ".")

    lines += ["", "### Resource cost (S1)", ""]
    for v in VARIANTS:
        ms = by.get(("steady", v), [])
        if not ms:
            lines.append(f"- {SHORT[v]}: NOT RUN")
            continue
        def med(key):
            summ = summarize([m[key] for m in ms])
            return summ["median"] if summ else None
        extra = f", Redis {fmt(med('redis_cpu'), 2)} cores / {fmt(med('redis_mem_mb'))} MiB" if v == "b" else ""
        lines.append(f"- {SHORT[v]}: aggregators {fmt(med('aggregator_cpu'), 2)} cores / {fmt(med('aggregator_mem_mb'))} MiB; "
                     f"Kafka broker {fmt(med('kafka_cpu'), 2)} cores / {fmt(med('kafka_mem_mb'))} MiB{extra}.")
    lines += ["", "Kafka Streams pays for state durability with changelog writes and one standby replica per task "
                  "(extra broker load and RocksDB memory in the aggregator pods); B pays with a separate Redis server.", ""]

    lines += ["## Recommendation", ""]
    verdicts = {}
    for v in VARIANTS:
        statuses = [m["slo_overall"] for (s, vv), lst in by.items() if vv == v and s not in ("smoke", "medium") for m in lst]
        verdicts[v] = combined_status(statuses)
    passing = [v for v in VARIANTS if verdicts[v] == "PASS"]
    if all(verdicts[v] == "NOT RUN" for v in VARIANTS):
        lines.append("No target-load scenarios have been run; no recommendation can be made yet.")
    elif len(passing) == 1:
        lines.append(f"Only **{SHORT[passing[0]]}** met the 500 ms p99.95 SLO with correct results in every executed "
                     "target-load scenario. Check the NOT RUN list above before treating this as final.")
    elif len(passing) > 1:
        lines.append("Several variants met the SLO in every executed scenario (" + ", ".join(SHORT[v] for v in passing) +
                     "); choose on operational trade-offs and resource cost (see docs/ARCHITECTURE.md).")
    else:
        lines.append("No variant met the 500 ms p99.95 SLO in every executed target-load scenario (" +
                     ", ".join(f"{SHORT[v]}: {verdicts[v]}" for v in VARIANTS) +
                     "), so none can be recommended as is for the stated requirement.")
        disruption = [sc for sc in ("rolling", "abrupt-pod", "cold-pod", "rescheduling") if (sc, "measure_p9995") in clear_winners]
        leaders = {clear_winners[(sc, "measure_p9995")] for sc in disruption}
        if disruption and len(leaders) == 1 and None not in leaders:
            leader = leaders.pop()
            lines += ["", f"Relative to each other, **{SHORT[leader]}** had the lowest p99.95 in every executed pod-disruption "
                          f"scenario ({', '.join(disruption)}), each time with non-overlapping repetition ranges and at least "
                          "10% difference. For the question “which variant processes most reliably under frequent pod "
                          f"rescheduling”, the measured answer on this environment is {SHORT[leader]}; it still exceeds the "
                          "500 ms p99.95 SLO, so meeting the SLO requires changes beyond the choice of variant."]
        else:
            lines += ["", "The pod-disruption scenarios (S2–S5) do not have one consistent leader; compare the scenario rows "
                          "above to see where each variant fails, and note INCONCLUSIVE runs that were limited by the laptop "
                          "rather than the design."]
        streams_lead = []
        for sc in ("rolling", "abrupt-pod", "cold-pod", "rescheduling"):
            pair = [summarize([m["measure_p9995"] for m in by.get((sc, v), []) if m["correctness"] == "PASS"
                               and m["load_achieved"] and not m["saturated"]]) for v in ("a1", "a2")]
            if all(pair):
                (a1, a2) = pair
                if a2["max"] < a1["min"] and (a1["median"] - a2["median"]) / a1["median"] >= 0.10:
                    streams_lead.append((sc, "a2"))
                elif a1["max"] < a2["min"] and (a2["median"] - a1["median"]) / a2["median"] >= 0.10:
                    streams_lead.append((sc, "a1"))
                else:
                    streams_lead.append((sc, None))
        if streams_lead:
            counts = {v: sum(1 for _, w in streams_lead if w == v) for v in ("a1", "a2")}
            lines += ["", f"Between the two Kafka Streams variants (identical code, different rebalance protocol): "
                          f"{SHORT['a2']} was clearly ahead in {counts['a2']} and {SHORT['a1']} in {counts['a1']} of "
                          f"{len(streams_lead)} pod-disruption scenarios (same rule: non-overlapping ranges, ≥ 10%)."]
        steady = clear_winners.get(("steady", "steady_p9995"), "missing")
        if steady is None:
            lines += ["", "Steady-state (S1) end-to-end tails do not separate the variants (overlapping ranges). Variant "
                          "differences inside the aggregation step are visible in the aggregation-only latency row; "
                          "`docs/ANALYSIS-NOTES.md` records where the end-to-end steady-state tails arise."]
    lines += ["", "## Limitations of this environment", "",
              "- Single OrbStack node: all pods share one VM, one disk and one kernel; node failures, cross-node "
              "networking, rack awareness and independent failure domains are not reproduced.",
              "- The local profile has one Kafka broker and one Redis node; broker failover and Redis replication are "
              "only exercised in the HA profile (`./scripts/deploy.sh --ha`).",
              "- Other workloads on the same cluster share CPU and memory; runs whose node CPU p95 exceeded 90% are "
              "marked INCONCLUSIVE.",
              "- All timestamps come from the same node clock; cross-machine clock skew is not present here but would be "
              "in production.",
              "- All variants write their actions to one shared PostgreSQL instance on the same VM, so stalls of that "
              "write path appear in every variant's end-to-end latency (aggregation-only latency excludes it).",
              "- Steady and failure windows are assigned by the broker append time of an action's source fragment; an "
              "action appended shortly before a fault and delayed by it counts as steady. Whole-window figures (used for "
              "S2–S5 above) do not depend on this rule; its effect per run is quantified by "
              "`scripts/analysis/window_sensitivity.py` (see `docs/ANALYSIS-NOTES.md`).", ""]
    return "\n".join(lines)


def main():
    runs = load_runs()
    runs_by_id = {r["id"]: r for r in runs}
    metrics, by, scenarios = build(runs)
    not_run = not_run_entries()
    os.makedirs(RESULTS, exist_ok=True)
    write_summary(metrics)
    results_text = results_md(metrics, by, scenarios, not_run)
    html_report(metrics, by, scenarios, runs_by_id, not_run, results_text)
    # docs/RESULTS.md is only written for the canonical results directory; trial result sets get their own copy.
    default_results = os.path.realpath(os.path.join(ROOT, "results")) == os.path.realpath(RESULTS)
    results_md_path = os.path.join(ROOT, "docs", "RESULTS.md") if default_results else os.path.join(RESULTS, "RESULTS.md")
    with open(results_md_path, "w") as f:
        f.write(results_text)
    print(f"report: {os.path.join(RESULTS, 'report.html')} ({len(metrics)} runs)")
    print(f"results: {results_md_path}")


if __name__ == "__main__":
    sys.exit(main())
