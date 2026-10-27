#!/usr/bin/env python3
"""Exports resource usage and recovery time series of one run from Prometheus into resources.json.

Resource figures come from the kubelet resource-metrics endpoint (container CPU / working-set memory, node CPU)
and from the JVMs (GC, heap), over the measurement window. Node CPU utilisation is used to flag
resource-saturated runs.
Usage: prom_export.py --url http://localhost:19090 --run-id ID --db-cmd scripts/lib/run_window.sh --out resources.json
"""
import argparse
import json
import subprocess
import sys
import urllib.parse
import urllib.request

GROUPS = [("aggregator", "aggregator-"), ("kafka", "kafka-"), ("redis", "redis-"), ("postgres", "postgres-"),
          ("persistence", "persistence-service-"), ("ml", "ml-simulator-"), ("generator", "load-generator-"),
          ("monitoring", "prometheus-"), ("monitoring", "grafana-")]


def query_range(url, expr, start, end, step):
    params = urllib.parse.urlencode({"query": expr, "start": start, "end": end, "step": step})
    with urllib.request.urlopen(f"{url}/api/v1/query_range?{params}", timeout=60) as response:
        body = json.load(response)
    if body.get("status") != "success":
        raise RuntimeError(body)
    return body["data"]["result"]


def stats(values):
    numbers = sorted(float(v) for _, v in values if v not in ("NaN", "+Inf", "-Inf"))
    if not numbers:
        return None
    return {"avg": sum(numbers) / len(numbers), "p95": numbers[min(len(numbers) - 1, int(0.95 * len(numbers)))],
            "max": numbers[-1]}


def fresh(selector, max_age_seconds=45):
    """Keeps only samples younger than max_age_seconds. Kubelet resource metrics carry explicit timestamps, so
    Prometheus writes no staleness marker when a pod disappears and its last value would otherwise be returned for
    another five minutes (the lookback window), e.g. the previous run's pods during this run's measurement."""
    return f"({selector} and (time() - timestamp({selector}) < {max_age_seconds}))"


def group_of(pod):
    for name, prefix in GROUPS:
        if pod.startswith(prefix):
            return name
    return "other"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--db-cmd", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--namespace", default="aggbench")
    args = parser.parse_args()

    window = subprocess.run([args.db_cmd, args.run_id], capture_output=True, text=True, check=True).stdout.split()
    start_us, measure_start_us, measure_end_us, finished_us = (int(x) for x in window)
    m_start, m_end = measure_start_us / 1e6, measure_end_us / 1e6
    r_start, r_end = start_us / 1e6 - 30, finished_us / 1e6 + 10
    ns = args.namespace
    container = f'namespace="{ns}",container!="",container!="POD"'
    result = {"runId": args.run_id, "measureStart": m_start, "measureEnd": m_end, "pods": {}, "groups": {}, "series": {}}

    cpu = query_range(args.url, f"sum by (pod) (rate(container_cpu_usage_seconds_total{{{container}}}[30s]))", m_start, m_end, "5s")
    mem = query_range(args.url, f"sum by (pod) ({fresh(f'container_memory_working_set_bytes{{{container}}}')})", m_start, m_end, "5s")
    gc = query_range(args.url, f'sum by (pod) (increase(jvm_gc_pause_seconds_sum{{namespace="{ns}"}}[30s]) or increase(jvm_gc_pause_seconds_sum[30s]))', m_start, m_end, "30s")
    gc_max = query_range(args.url, "max by (pod) (jvm_gc_pause_seconds_max)", m_start, m_end, "5s")
    heap = query_range(args.url, 'sum by (pod) (jvm_memory_used_bytes{area="heap"})', m_start, m_end, "5s")
    for series, key in ((cpu, "cpuCores"), (mem, "memoryWorkingSetBytes"), (gc, "gcPauseSecondsPer30s"),
                        (gc_max, "gcMaxPauseSeconds"), (heap, "heapUsedBytes")):
        for s in series:
            pod = s["metric"].get("pod", "unknown")
            entry = result["pods"].setdefault(pod, {"group": group_of(pod)})
            entry[key] = stats(s["values"])

    # Group figures are statistics of the instantaneous group total, so pods replaced during the window are not
    # counted twice (a killed pod and its replacement never add up to more than what ran at one time).
    for pod, entry in result["pods"].items():
        result["groups"].setdefault(entry["group"], {"pods": 0})["pods"] += 1
    for name, g in result["groups"].items():
        prefixes = "|".join(prefix for group, prefix in GROUPS if group == name)
        selector = f'{container},pod=~"({prefixes}).*"' if prefixes else None
        if selector is None:
            continue
        total_cpu = query_range(args.url, f"sum(rate(container_cpu_usage_seconds_total{{{selector}}}[30s]))", m_start, m_end, "5s")
        total_mem = query_range(args.url, f"sum({fresh(f'container_memory_working_set_bytes{{{selector}}}')})", m_start, m_end, "5s")
        cpu_stats = stats(total_cpu[0]["values"]) if total_cpu else None
        mem_stats = stats(total_mem[0]["values"]) if total_mem else None
        g["cpuCoresAvg"] = cpu_stats["avg"] if cpu_stats else 0.0
        g["cpuCoresMax"] = cpu_stats["max"] if cpu_stats else 0.0
        g["memoryMaxBytes"] = mem_stats["max"] if mem_stats else 0.0

    node = query_range(args.url, "sum(rate(node_cpu_usage_seconds_total[30s])) / sum(machine_cpu_cores)", m_start, m_end, "5s")
    result["nodeCpuUtilization"] = stats(node[0]["values"]) if node else None

    timelines = {
        "aggregatorCpuCores": f'sum(rate(container_cpu_usage_seconds_total{{{container},pod=~"aggregator-.*"}}[15s]))',
        "streamsActiveTasks": "sum(bench_streams_active_tasks)",
        "streamsStandbyTasks": "sum(bench_streams_standby_tasks)",
        "streamsRestoringPartitions": "sum(bench_streams_restoring_partitions)",
        "streamsRestoredRecordsPerSecond": "sum(rate(bench_streams_restored_records_total[10s]))",
        "streamsMaxStandbyLag": "max(bench_streams_standby_lag_records)",
        "assignedPartitions": 'sum(bench_consumer_assigned_partitions{loop="ingress"})',
        "redisMergeP99Seconds": "histogram_quantile(0.99, sum by (le) (rate(bench_redis_merge_seconds_bucket[10s])))",
        "redisUsedMemoryBytes": "max(redis_memory_used_bytes)",
        "actionsInFlight": "sum(bench_actions_inflight)",
        "persistenceBatchMeanSeconds": "sum(rate(bench_persistence_batch_seconds_sum[4s])) / sum(rate(bench_persistence_batch_seconds_count[4s]))",
        "persistenceQueueDepth": "sum(bench_persistence_queue_depth)",
        "consumersPaused": "sum(bench_consumer_paused)",
        "inferenceQueued": "sum(bench_inference_queued)",
        "nodeCpuUtilization": "sum(rate(node_cpu_usage_seconds_total[15s])) / sum(machine_cpu_cores)",
    }
    for name, expr in timelines.items():
        try:
            series = query_range(args.url, expr, r_start, r_end, "2s")
            result["series"][name] = [[round(float(t) - m_start, 1), float(v)] for t, v in series[0]["values"]] if series else []
        except Exception as error:  # a missing metric family is not fatal
            result["series"][name] = []
            print(f"series {name}: {error}", file=sys.stderr)

    with open(args.out, "w") as handle:
        json.dump(result, handle, indent=2)


if __name__ == "__main__":
    main()
