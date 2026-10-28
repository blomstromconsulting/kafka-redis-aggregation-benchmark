package io.aggbench.verifier;

import io.aggbench.verifier.Stats.LongList;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Latency, throughput, backlog and recovery analysis of one run, computed from durable outputs:
 * <ul>
 *   <li>Latency = durable commit time (persistence service, after COMMIT) − broker append time of the latest
 *       fragment the action depends on. Exact percentiles over every sample.</li>
 *   <li>Aggregation-only latency = executor pickup time − same broker append time (STAGE2/FINAL).</li>
 *   <li>Result-ready latency = the moment the action's result exists, before it is persisted − same broker append
 *       time: executor pickup for INDIVIDUAL/STAGE2, pickup + ML inference (including its queueing) for FINAL. It
 *       is the end-to-end latency without the persistence step (whose real-world equivalent is publishing the
 *       result).</li>
 *   <li>Steady vs failure windows: a failure window starts at each injected fault ({@code FAULT_*} events written
 *       by chaos.sh) and lasts {@code faultWindowSeconds}.</li>
 * </ul>
 * All timestamps share the single node's wall clock (see docs/BENCHMARK-METHODOLOGY.md for skew assumptions).
 */
final class PerformanceAnalysis {

    static final String[] KINDS = {"INDIVIDUAL", "STAGE2", "FINAL"};

    private final Connection db;
    private final String runId;
    private final long sloMs;
    private final long faultWindowUs;

    // Column store of all action outcomes.
    private byte[] kind = new byte[1 << 16];
    private long[] src = new long[1 << 16];
    private long[] first = new long[1 << 16];
    private long[] ready = new long[1 << 16];
    private long[] resultReady = new long[1 << 16];
    private long[] done = new long[1 << 16];
    private int[] partition = new int[1 << 16];
    private final Map<Integer, String> slowCorrelationIds = new HashMap<>();
    private boolean[] inFailureWindow;
    private int size;

    record Fault(long tsUs, String type, String detail) {
    }

    PerformanceAnalysis(Connection db, String runId, long sloMs, long faultWindowSeconds) {
        this.db = db;
        this.runId = runId;
        this.sloMs = sloMs;
        this.faultWindowUs = faultWindowSeconds * 1_000_000L;
    }

    Map<String, Object> run(Map<String, Object> runRow, Path outDir) throws SQLException, IOException {
        load();
        long measureStart = (long) runRow.get("measure_start_us");
        long measureEnd = (long) runRow.get("measure_end_us");
        long runStart = (long) runRow.get("start_us");
        List<Fault> faults = faults(measureStart);
        inFailureWindow = new boolean[size];
        for (int i = 0; i < size; i++) {
            for (Fault fault : faults) {
                if (src[i] >= fault.tsUs() && src[i] < fault.tsUs() + faultWindowUs) {
                    inFailureWindow[i] = true;
                    break;
                }
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("samples", size);
        result.put("faults", faults.size());

        Map<String, Object> latency = new LinkedHashMap<>();
        latency.put("measure", latencyStats(measureStart, measureEnd, faults, Window.ALL, Metric.E2E));
        latency.put("steady", latencyStats(measureStart, measureEnd, faults, Window.STEADY, Metric.E2E));
        latency.put("failure", faults.isEmpty() ? Map.of() : latencyStats(measureStart, measureEnd, faults, Window.FAILURE, Metric.E2E));
        latency.put("aggregationOnly", latencyStats(measureStart, measureEnd, faults, Window.ALL, Metric.AGGREGATION));
        latency.put("aggregationOnlySteady", latencyStats(measureStart, measureEnd, faults, Window.STEADY, Metric.AGGREGATION));
        latency.put("firstFragmentToFinal", latencyStats(measureStart, measureEnd, faults, Window.ALL, Metric.FIRST_TO_FINAL));
        latency.put("resultMeasure", latencyStats(measureStart, measureEnd, faults, Window.ALL, Metric.RESULT));
        latency.put("resultSteady", latencyStats(measureStart, measureEnd, faults, Window.STEADY, Metric.RESULT));
        latency.put("resultFailure", faults.isEmpty() ? Map.of() : latencyStats(measureStart, measureEnd, faults, Window.FAILURE, Metric.RESULT));
        result.put("latency", latency);

        result.put("throughput", throughput(measureStart, measureEnd, runRow));
        Map<String, Object> lag = lag(measureStart, measureEnd);
        result.put("lag", lag);
        result.put("events", eventSummary(runStart, Long.MAX_VALUE));
        List<Map<String, Object>> faultResults = new ArrayList<>();
        for (int i = 0; i < faults.size(); i++) {
            Fault fault = faults.get(i);
            long end = Math.min(fault.tsUs() + faultWindowUs, i + 1 < faults.size() ? faults.get(i + 1).tsUs() : Long.MAX_VALUE);
            faultResults.add(faultRecovery(fault, end, measureStart, faults));
        }
        result.put("faultRecovery", faultResults);

        writePerSecond(outDir.resolve("per_second_latency.csv"), runStart, measureStart);
        writeRawLatencies(outDir.resolve("latencies.csv.gz"), measureStart);
        writeLagCsv(outDir.resolve("lag.csv"), measureStart);
        writeTimeline(outDir.resolve("timeline.csv"), measureStart);
        return result;
    }

    private enum Window { ALL, STEADY, FAILURE }

    private enum Metric { E2E, AGGREGATION, FIRST_TO_FINAL, RESULT }

    private void load() throws SQLException {
        db.setAutoCommit(false);
        String sql = """
                SELECT r.kind, r.source_append_us, r.first_append_us, r.ready_us,
                       coalesce(b.postcommit_us, r.precommit_us), r.ingress_partition, r.correlation_id, r.ready_us
                FROM action_receipts r LEFT JOIN commit_batches b ON b.batch_id = r.batch_id WHERE r.run_id = ?
                UNION ALL
                SELECT 'FINAL', a.source_append_us, a.first_append_us, a.ready_us,
                       coalesce(b.postcommit_us, a.precommit_us), a.ingress_partition, a.correlation_id,
                       a.ready_us + coalesce(a.inference_us, 0)
                FROM aggregates a LEFT JOIN commit_batches b ON b.batch_id = a.batch_id WHERE a.run_id = ?""";
        try (PreparedStatement ps = db.prepareStatement(sql)) {
            ps.setFetchSize(20_000);
            ps.setString(1, runId);
            ps.setString(2, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ensureCapacity();
                    kind[size] = (byte) switch (rs.getString(1)) {
                        case "INDIVIDUAL" -> 0;
                        case "STAGE2" -> 1;
                        default -> 2;
                    };
                    src[size] = rs.getLong(2);
                    first[size] = rs.getLong(3);
                    ready[size] = rs.getLong(4);
                    done[size] = rs.getLong(5);
                    partition[size] = rs.getInt(6);
                    resultReady[size] = rs.getLong(8);
                    if (done[size] - src[size] > sloMs * 1000) {
                        slowCorrelationIds.put(size, rs.getString(7));
                    }
                    size++;
                }
            }
        } finally {
            db.setAutoCommit(true);
        }
    }

    private void ensureCapacity() {
        if (size == kind.length) {
            int n = size * 2;
            kind = Arrays.copyOf(kind, n);
            src = Arrays.copyOf(src, n);
            first = Arrays.copyOf(first, n);
            ready = Arrays.copyOf(ready, n);
            resultReady = Arrays.copyOf(resultReady, n);
            done = Arrays.copyOf(done, n);
            partition = Arrays.copyOf(partition, n);
        }
    }

    private List<Fault> faults(long measureStart) throws SQLException {
        List<Fault> faults = new ArrayList<>();
        try (PreparedStatement ps = db.prepareStatement(
                "SELECT ts_us, type, detail::text FROM bench_events WHERE run_id = ? AND type LIKE 'FAULT_%' ORDER BY ts_us")) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    faults.add(new Fault(rs.getLong(1), rs.getString(2), rs.getString(3)));
                }
            }
        }
        return faults;
    }

    private Map<String, Object> latencyStats(long from, long to, List<Fault> faults, Window window, Metric metric) {
        Map<String, Object> byKind = new LinkedHashMap<>();
        LongList all = new LongList();
        for (int k = 0; k < KINDS.length; k++) {
            if (metric == Metric.AGGREGATION && k == 0 || metric == Metric.FIRST_TO_FINAL && k != 2) {
                continue;
            }
            LongList values = new LongList();
            for (int i = 0; i < size; i++) {
                if (kind[i] != k || src[i] < from || src[i] >= to) {
                    continue;
                }
                boolean failure = inFailureWindow[i];
                if (window == Window.STEADY && failure || window == Window.FAILURE && !failure) {
                    continue;
                }
                long value = switch (metric) {
                    case E2E -> done[i] - src[i];
                    case AGGREGATION -> ready[i] - src[i];
                    case FIRST_TO_FINAL -> done[i] - first[i];
                    case RESULT -> resultReady[i] - src[i];
                };
                values.add(Math.max(0, value));
                all.add(Math.max(0, value));
            }
            byKind.put(KINDS[k], Stats.summarize(values.values, values.size, sloMs));
        }
        if (metric == Metric.E2E || metric == Metric.RESULT) {
            byKind.put("ALL", Stats.summarize(all.values, all.size, sloMs));
        }
        return byKind;
    }

    private Map<String, Object> throughput(long measureStart, long measureEnd, Map<String, Object> runRow) throws SQLException {
        Map<String, Object> result = new LinkedHashMap<>();
        TreeMap<Long, Integer> completionsPerSecond = new TreeMap<>();
        long ingressInWindow = 0;
        for (int i = 0; i < size; i++) {
            if (kind[i] != 0) {
                continue;
            }
            if (src[i] >= measureStart && src[i] < measureEnd) {
                ingressInWindow++;
            }
            if (done[i] >= measureStart + 5_000_000 && done[i] < measureEnd) {
                completionsPerSecond.merge((done[i] - measureStart) / 1_000_000, 1, Integer::sum);
            }
        }
        double seconds = (measureEnd - measureStart) / 1e6;
        result.put("individualActionsPerSecondMean", ingressInWindow / seconds);
        int[] perSecond = completionsPerSecond.values().stream().mapToInt(Integer::intValue).sorted().toArray();
        result.put("individualActionsPerSecondMedian", perSecond.length == 0 ? 0 : perSecond[perSecond.length / 2]);
        result.put("individualActionsPerSecondMin", perSecond.length == 0 ? 0 : perSecond[0]);
        try (PreparedStatement ps = db.prepareStatement("""
                SELECT coalesce(sum(acked), 0)::float8 / greatest(count(*), 1), coalesce(max(max_schedule_lag_ms), 0),
                       coalesce(avg(requested_rate), 0)
                FROM producer_samples WHERE run_id = ? AND phase = 'measure'""")) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                result.put("producerAckedPerSecondMeasure", rs.getDouble(1));
                result.put("producerMaxScheduleLagMsMeasure", rs.getLong(2));
                result.put("producerRequestedRateMeasure", rs.getDouble(3));
            }
        }
        result.put("producerAchievedRateWholeRun", runRow.get("achieved_rate"));
        return result;
    }

    private Map<String, Object> lag(long measureStart, long measureEnd) throws SQLException {
        Map<String, Object> result = new LinkedHashMap<>();
        try (PreparedStatement ps = db.prepareStatement("""
                SELECT group_id, max(lag), max(oldest_age_ms),
                       percentile_disc(0.5) WITHIN GROUP (ORDER BY lag),
                       percentile_disc(0.95) WITHIN GROUP (ORDER BY lag)
                FROM lag_samples WHERE run_id = ? AND ts_us BETWEEN ? AND ? GROUP BY group_id""")) {
            ps.setString(1, runId);
            ps.setLong(2, measureStart);
            ps.setLong(3, measureEnd);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getString(1), Map.of("maxLag", rs.getLong(2), "maxOldestUnprocessedAgeMs", rs.getLong(3),
                            "medianLag", rs.getLong(4), "p95Lag", rs.getLong(5)));
                }
            }
        }
        return result;
    }

    private Map<String, Object> eventSummary(long from, long to) throws SQLException {
        Map<String, Object> result = new TreeMap<>();
        try (PreparedStatement ps = db.prepareStatement("""
                SELECT type, count(*),
                       coalesce(sum((detail->>'recordsRestored')::bigint), 0),
                       coalesce(max((detail->>'durationMs')::bigint), 0),
                       coalesce(max((detail->>'msSinceTaskInit')::bigint), 0),
                       coalesce(max((detail->>'firstRecordAgeMs')::bigint), 0),
                       coalesce(max((detail->>'msSinceAssignment')::bigint), 0)
                FROM bench_events WHERE run_id = ? AND ts_us BETWEEN ? AND ? GROUP BY type""")) {
            ps.setString(1, runId);
            ps.setLong(2, from);
            ps.setLong(3, to);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> e = new LinkedHashMap<>();
                    e.put("count", rs.getLong(2));
                    if (rs.getString(1).startsWith("RESTORE_END")) {
                        e.put("recordsRestored", rs.getLong(3));
                        e.put("maxDurationMs", rs.getLong(4));
                    }
                    if (rs.getString(1).equals("TASK_PROCESSING_STARTED")) {
                        e.put("maxMsSinceTaskInit", rs.getLong(5));
                        e.put("maxFirstRecordAgeMs", rs.getLong(6));
                    }
                    if (rs.getString(1).equals("PARTITION_PROCESSING_STARTED")) {
                        e.put("maxMsSinceAssignment", rs.getLong(7));
                        e.put("maxFirstRecordAgeMs", rs.getLong(6));
                    }
                    result.put(rs.getString(1), e);
                }
            }
        }
        return result;
    }

    private Map<String, Object> faultRecovery(Fault fault, long windowEnd, long measureStart, List<Fault> faults) throws SQLException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("type", fault.type());
        result.put("detail", fault.detail());
        result.put("atSecond", (fault.tsUs() - measureStart) / 1e6);
        result.put("windowSeconds", (windowEnd - fault.tsUs()) / 1e6);

        // Latency inside this fault's window (by arrival time).
        LongList all = new LongList();
        List<Map<String, Object>> slow = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (src[i] >= fault.tsUs() && src[i] < windowEnd) {
                all.add(done[i] - src[i]);
                String corr = slowCorrelationIds.get(i);
                if (corr != null && kind[i] == 2 && slow.size() < 20) {
                    slow.add(Map.of("correlationId", corr, "partition", partition[i], "latencyMs", Stats.ms(done[i] - src[i])));
                }
            }
        }
        result.put("latency", Stats.summarize(all.values, all.size, sloMs));
        result.put("sampleSlowFinalAggregates", slow);
        long slowFinals = 0;
        for (Map.Entry<Integer, String> e : slowCorrelationIds.entrySet()) {
            int i = e.getKey();
            if (kind[i] == 2 && src[i] >= fault.tsUs() && src[i] < windowEnd) {
                slowFinals++;
            }
        }
        result.put("finalAggregatesOverSlo", slowFinals);

        // Processing stalls per ingress partition and latency recovery, on both bases (durable commit; result ready).
        result.putAll(partitionStalls(done, fault, windowEnd, ""));
        result.put("latencyRecoverySeconds", latencyRecovery(done, fault.tsUs(), windowEnd));
        LongList resultAll = new LongList();
        for (int i = 0; i < size; i++) {
            if (src[i] >= fault.tsUs() && src[i] < windowEnd) {
                resultAll.add(resultReady[i] - src[i]);
            }
        }
        result.put("resultLatency", Stats.summarize(resultAll.values, resultAll.size, sloMs));
        result.putAll(partitionStalls(resultReady, fault, windowEnd, "result"));
        result.put("resultLatencyRecoverySeconds", latencyRecovery(resultReady, fault.tsUs(), windowEnd));
        result.put("backlog", backlogRecovery(fault.tsUs(), windowEnd, measureStart, faults));
        result.put("events", eventSummary(fault.tsUs(), windowEnd));
        return result;
    }

    /**
     * Largest gap between consecutive completions per ingress partition after the fault (gaps over 2 s are listed).
     * {@code completed} is the durable commit time or the result-ready time; keys get {@code prefix} (camel case).
     */
    private Map<String, Object> partitionStalls(long[] completed, Fault fault, long windowEnd, String prefix) {
        Map<Integer, LongList> completions = new TreeMap<>();
        long from = fault.tsUs() - 10_000_000L;
        long to = Math.max(windowEnd, fault.tsUs() + 30_000_000L);
        for (int i = 0; i < size; i++) {
            if (completed[i] >= from && completed[i] < to) {
                completions.computeIfAbsent(partition[i], p -> new LongList()).add(completed[i]);
            }
        }
        List<Map<String, Object>> stalls = new ArrayList<>();
        long maxResumeMs = 0;
        for (Map.Entry<Integer, LongList> entry : completions.entrySet()) {
            long[] times = Arrays.copyOf(entry.getValue().values, entry.getValue().size);
            Arrays.sort(times);
            long maxGap = 0;
            long gapEnd = 0;
            for (int i = 1; i < times.length; i++) {
                if (times[i] - times[i - 1] > maxGap && times[i] >= fault.tsUs()) {
                    maxGap = times[i] - times[i - 1];
                    gapEnd = times[i];
                }
            }
            if (maxGap > 2_000_000L) {
                long resumeMs = (gapEnd - fault.tsUs()) / 1000;
                maxResumeMs = Math.max(maxResumeMs, resumeMs);
                stalls.add(Map.of("partition", entry.getKey(), "stallMs", maxGap / 1000, "resumedAfterFaultMs", resumeMs));
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(prefix.isEmpty() ? "stalledPartitions" : prefix + "StalledPartitions", stalls);
        result.put(prefix.isEmpty() ? "maxPartitionResumeMs" : prefix + "MaxPartitionResumeMs", maxResumeMs);
        return result;
    }

    /** First second after the fault from which the per-second p99 stays within the SLO for 10 s. */
    private Object latencyRecovery(long[] completed, long faultUs, long windowEnd) {
        TreeMap<Long, LongList> perSecond = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            if (src[i] >= faultUs && src[i] < windowEnd + 60_000_000L) {
                perSecond.computeIfAbsent((src[i] - faultUs) / 1_000_000, s -> new LongList()).add(completed[i] - src[i]);
            }
        }
        List<Long> seconds = new ArrayList<>(perSecond.keySet());
        int okStreak = 0;
        for (int i = 0; i < seconds.size(); i++) {
            LongList list = perSecond.get(seconds.get(i));
            long[] sorted = Arrays.copyOf(list.values, list.size);
            Arrays.sort(sorted);
            boolean ok = sorted[Stats.rank(0.99, sorted.length)] <= sloMs * 1000;
            okStreak = ok ? okStreak + 1 : 0;
            if (okStreak == 10) {
                return seconds.get(i - 9);
            }
        }
        return "not recovered within window";
    }

    private Map<String, Object> backlogRecovery(long faultUs, long windowEnd, long measureStart, List<Fault> faults) throws SQLException {
        Map<String, Object> result = new LinkedHashMap<>();
        long firstFault = faults.isEmpty() ? Long.MAX_VALUE : faults.getFirst().tsUs();
        try (PreparedStatement ps = db.prepareStatement("""
                SELECT group_id, ts_us, lag, oldest_age_ms FROM lag_samples
                WHERE run_id = ? AND ts_us BETWEEN ? AND ? ORDER BY group_id, ts_us""")) {
            ps.setString(1, runId);
            ps.setLong(2, measureStart);
            ps.setLong(3, windowEnd + 120_000_000L);
            Map<String, List<long[]>> byGroup = new LinkedHashMap<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    byGroup.computeIfAbsent(rs.getString(1), g -> new ArrayList<>())
                            .add(new long[]{rs.getLong(2), rs.getLong(3), rs.getLong(4)});
                }
            }
            for (Map.Entry<String, List<long[]>> entry : byGroup.entrySet()) {
                LongList baseline = new LongList();
                long peak = 0;
                long peakAge = 0;
                for (long[] s : entry.getValue()) {
                    if (s[0] < Math.min(firstFault, faultUs)) {
                        baseline.add(s[1]);
                    }
                    if (s[0] >= faultUs && s[0] < windowEnd) {
                        peak = Math.max(peak, s[1]);
                        peakAge = Math.max(peakAge, s[2]);
                    }
                }
                long[] sorted = Arrays.copyOf(baseline.values, baseline.size);
                Arrays.sort(sorted);
                long baseP95 = sorted.length == 0 ? 0 : sorted[Stats.rank(0.95, sorted.length)];
                long threshold = Math.max(baseP95 * 2, 100);
                Object recovered = "not recovered";
                int streak = 0;
                boolean pastPeak = false;
                for (long[] s : entry.getValue()) {
                    if (s[0] < faultUs) {
                        continue;
                    }
                    if (s[1] == peak) {
                        pastPeak = true;
                    }
                    if (!pastPeak) {
                        continue;
                    }
                    streak = s[1] <= threshold ? streak + 1 : 0;
                    if (streak == 5) {
                        recovered = (s[0] - faultUs) / 1e6 - 4;
                        break;
                    }
                }
                result.put(entry.getKey(), Map.of("baselineP95Lag", baseP95, "peakLag", peak,
                        "peakOldestUnprocessedAgeMs", peakAge, "recoveredToBaselineAfterSeconds", recovered));
            }
        }
        return result;
    }

    private void writePerSecond(Path file, long runStart, long measureStart) throws IOException {
        TreeMap<Long, LongList[]> buckets = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            long second = Math.floorDiv(src[i] - measureStart, 1_000_000L);
            LongList[] lists = buckets.computeIfAbsent(second, s -> new LongList[]{new LongList(), new LongList(), new LongList(), new LongList()});
            lists[kind[i]].add(done[i] - src[i]);
            lists[3].add(done[i] - src[i]);
        }
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(file))) {
            out.println("second,kind,count,p50_ms,p99_ms,p99_95_ms,max_ms,over_slo");
            String[] names = {"INDIVIDUAL", "STAGE2", "FINAL", "ALL"};
            for (Map.Entry<Long, LongList[]> entry : buckets.entrySet()) {
                for (int k = 0; k < 4; k++) {
                    LongList list = entry.getValue()[k];
                    if (list.size == 0) {
                        continue;
                    }
                    long[] sorted = Arrays.copyOf(list.values, list.size);
                    Arrays.sort(sorted);
                    int over = 0;
                    for (long v : sorted) {
                        if (v > sloMs * 1000) {
                            over++;
                        }
                    }
                    out.printf("%d,%s,%d,%.2f,%.2f,%.2f,%.2f,%d%n", entry.getKey(), names[k], sorted.length,
                            Stats.ms(sorted[Stats.rank(0.5, sorted.length)]), Stats.ms(sorted[Stats.rank(0.99, sorted.length)]),
                            Stats.ms(sorted[Stats.rank(0.9995, sorted.length)]), Stats.ms(sorted[sorted.length - 1]), over);
                }
            }
        }
    }

    /** Every action outcome (compressed), so that any statistic can be recomputed offline. */
    private void writeRawLatencies(Path file, long measureStart) throws IOException {
        String[] names = {"INDIVIDUAL", "STAGE2", "FINAL"};
        try (PrintWriter out = new PrintWriter(new java.io.OutputStreamWriter(
                new java.util.zip.GZIPOutputStream(Files.newOutputStream(file)), java.nio.charset.StandardCharsets.UTF_8))) {
            out.println("kind,partition,append_s,ready_ms,done_ms,first_append_s,result_ms");
            for (int i = 0; i < size; i++) {
                out.printf("%s,%d,%.6f,%.3f,%.3f,%.6f,%.3f%n", names[kind[i]], partition[i], (src[i] - measureStart) / 1e6,
                        (ready[i] - src[i]) / 1000.0, (done[i] - src[i]) / 1000.0, (first[i] - measureStart) / 1e6,
                        (resultReady[i] - src[i]) / 1000.0);
            }
        }
    }

    private void writeLagCsv(Path file, long measureStart) throws SQLException, IOException {
        try (PreparedStatement ps = db.prepareStatement(
                "SELECT ts_us, group_id, lag, oldest_age_ms FROM lag_samples WHERE run_id = ? ORDER BY ts_us");
             PrintWriter out = new PrintWriter(Files.newBufferedWriter(file))) {
            ps.setString(1, runId);
            out.println("second,group,lag,oldest_age_ms");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.printf("%.1f,%s,%d,%d%n", (rs.getLong(1) - measureStart) / 1e6, rs.getString(2), rs.getLong(3), rs.getLong(4));
                }
            }
        }
        try (PreparedStatement ps = db.prepareStatement(
                "SELECT ts_us, phase, requested_rate, sent, acked, failed, max_schedule_lag_ms FROM producer_samples WHERE run_id = ? ORDER BY ts_us");
             PrintWriter out = new PrintWriter(Files.newBufferedWriter(file.resolveSibling("producer.csv")))) {
            ps.setString(1, runId);
            out.println("second,phase,requested_rate,sent,acked,failed,max_schedule_lag_ms");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.printf("%.1f,%s,%.1f,%d,%d,%d,%d%n", (rs.getLong(1) - measureStart) / 1e6, rs.getString(2),
                            rs.getDouble(3), rs.getLong(4), rs.getLong(5), rs.getLong(6), rs.getLong(7));
                }
            }
        }
        TreeMap<Long, Integer> completions = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            if (kind[i] == 0) {
                completions.merge(Math.floorDiv(done[i] - measureStart, 1_000_000L), 1, Integer::sum);
            }
        }
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(file.resolveSibling("throughput.csv")))) {
            out.println("second,individual_actions_completed");
            completions.forEach((second, count) -> out.printf("%d,%d%n", second, count));
        }
    }

    private void writeTimeline(Path file, long measureStart) throws SQLException, IOException {
        try (PreparedStatement ps = db.prepareStatement(
                "SELECT ts_us, source, type, detail::text FROM bench_events WHERE run_id = ? ORDER BY ts_us");
             PrintWriter out = new PrintWriter(Files.newBufferedWriter(file))) {
            ps.setString(1, runId);
            out.println("second,source,type,detail");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.printf("%.3f,%s,%s,\"%s\"%n", (rs.getLong(1) - measureStart) / 1e6, rs.getString(2), rs.getString(3),
                            rs.getString(4).replace("\"", "\"\""));
                }
            }
        }
    }
}
