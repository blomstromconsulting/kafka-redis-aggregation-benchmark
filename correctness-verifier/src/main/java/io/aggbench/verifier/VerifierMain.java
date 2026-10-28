package io.aggbench.verifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Independent verification of one benchmark run, executed outside the cluster (scripts/benchmark.sh runs it
 * through a port-forward to PostgreSQL). Writes {@code verification.json} plus per-second CSV series.
 *
 * <pre>
 * java -jar correctness-verifier.jar --jdbc-url jdbc:postgresql://localhost:15432/aggbench --user aggbench
 *      --password ... --run-id r123 --out results/runs/r123 [--slo-ms 500] [--fault-window-seconds 60]
 * </pre>
 *
 * Exit code: 0 = correctness PASS, 1 = correctness FAIL, 2 = INCONCLUSIVE, 3 = error.
 */
public final class VerifierMain {

    private VerifierMain() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parse(args);
        String runId = require(options, "run-id");
        Path out = Path.of(require(options, "out"));
        long sloMs = Long.parseLong(options.getOrDefault("slo-ms", "500"));
        long faultWindow = Long.parseLong(options.getOrDefault("fault-window-seconds", "60"));
        Files.createDirectories(out);

        try (Connection db = DriverManager.getConnection(require(options, "jdbc-url"), require(options, "user"),
                require(options, "password"))) {
            try (var statement = db.createStatement()) {
                // Fresh statistics for the rows just written, and hash joins for these whole-run analytical queries
                // (stale statistics otherwise produce nested-loop anti-joins that are quadratic in the run size).
                statement.execute("ANALYZE manifest, action_receipts, aggregates, commit_batches");
                statement.execute("SET enable_nestloop = off");
                // Parallel workers need large /dev/shm segments in the PostgreSQL pod; a single worker is fast enough.
                statement.execute("SET max_parallel_workers_per_gather = 0");
            }
            Map<String, Object> run = runRow(db, runId);
            if (run == null) {
                System.err.println("Run " + runId + " not found");
                System.exit(3);
            }
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("runId", runId);
            result.put("variant", run.get("variant"));
            result.put("scenario", run.get("scenario"));
            result.put("repetition", run.get("repetition"));
            result.put("runStatus", run.get("status"));
            result.put("sloMs", sloMs);
            result.put("config", new ObjectMapper().readValue((String) run.get("config"), Map.class));

            boolean drained = Boolean.TRUE.equals(run.get("drained"));
            long sendFailures = run.get("send_failures") == null ? -1 : (long) run.get("send_failures");
            Map<String, Object> correctness = new CorrectnessCheck(db, runId).run(drained, sendFailures);
            result.put("correctness", correctness);

            Map<String, Object> performance = new PerformanceAnalysis(db, runId, sloMs, faultWindow).run(run, out);
            Map<String, Object> load = load(run, performance);
            result.put("load", load);
            result.putAll(performance);
            result.put("slo", SloAssessment.assess(load, correctness, performance, sloMs));
            result.put("sloResultReady", SloAssessment.assess(load, correctness, performance, sloMs, "resultSteady", "resultFailure"));

            ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
            mapper.writeValue(out.resolve("verification.json").toFile(), result);
            System.out.println(mapper.writeValueAsString(Map.of(
                    "runId", runId, "correctness", correctness.get("status"),
                    "correctnessFailures", correctness.get("failures"), "slo", result.get("slo"))));
            String status = (String) correctness.get("status");
            System.exit(switch (status) {
                case "PASS" -> 0;
                case "FAIL" -> 1;
                default -> 2;
            });
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> load(Map<String, Object> run, Map<String, Object> performance) {
        Map<String, Object> throughput = (Map<String, Object>) performance.get("throughput");
        Map<String, Object> load = new LinkedHashMap<>();
        double requested = ((Number) run.get("requested_rate")).doubleValue();
        double achieved = ((Number) throughput.get("producerAckedPerSecondMeasure")).doubleValue();
        long scheduleLag = ((Number) throughput.get("producerMaxScheduleLagMsMeasure")).longValue();
        load.put("requestedRate", requested);
        load.put("achievedRateMeasure", achieved);
        load.put("maxScheduleLagMsMeasure", scheduleLag);
        load.put("eventsSent", run.get("events_sent"));
        load.put("eventsAcked", run.get("events_acked"));
        load.put("sendFailures", run.get("send_failures"));
        load.put("drained", run.get("drained"));
        boolean achievedOk = achieved >= 0.98 * requested && scheduleLag <= 1000 && Long.valueOf(0).equals(run.get("send_failures"));
        load.put("loadAchieved", achievedOk);
        return load;
    }

    private static Map<String, Object> runRow(Connection db, String runId) throws Exception {
        try (PreparedStatement ps = db.prepareStatement("SELECT *, config::text AS config FROM runs WHERE run_id = ?")) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    row.put(rs.getMetaData().getColumnLabel(i), rs.getObject(i));
                }
                return row;
            }
        }
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument " + args[i]);
            }
            options.put(args[i].substring(2), args[i + 1]);
        }
        return options;
    }

    private static String require(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null) {
            throw new IllegalArgumentException("Missing --" + name + "; usage: " + List.of("--jdbc-url", "--user", "--password", "--run-id", "--out"));
        }
        return value;
    }
}
