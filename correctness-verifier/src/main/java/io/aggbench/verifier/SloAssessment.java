package io.aggbench.verifier;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * PASS / FAIL / INCONCLUSIVE classification of one run. Correctness and latency are reported separately; a run
 * that lost nothing but recovered slowly is "correctness PASS, latency FAIL".
 *
 * <p>Latency target: p99.95 of every end-to-end path (INDIVIDUAL, STAGE2, FINAL) at most the SLO. Steady-state
 * and failure windows are assessed separately. Fewer than 2,000 samples in a window cannot support a p99.95
 * claim (the estimate would rest on a single sample) and yields INCONCLUSIVE. The same rules are applied a second
 * time to the result-ready latency (without the persistence step), see {@link PerformanceAnalysis}.
 */
final class SloAssessment {

    private static final int MIN_SAMPLES_FOR_P9995 = 2000;

    private SloAssessment() {
    }

    /** End-to-end latency (to the durable commit). */
    static Map<String, Object> assess(Map<String, Object> load, Map<String, Object> correctness,
                                      Map<String, Object> performance, long sloMs) {
        return assess(load, correctness, performance, sloMs, "steady", "failure");
    }

    /** Latency read from {@code latency.<steadyKey>} and {@code latency.<failureKey>}. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> assess(Map<String, Object> load, Map<String, Object> correctness,
                                      Map<String, Object> performance, long sloMs, String steadyKey, String failureKey) {
        Map<String, Object> latency = (Map<String, Object>) performance.get("latency");
        Map<String, Object> result = new LinkedHashMap<>();
        boolean loadOk = Boolean.TRUE.equals(load.get("loadAchieved"));
        String correctnessStatus = (String) correctness.get("status");
        String steady = latencyStatus((Map<String, Object>) latency.get(steadyKey), sloMs);
        Map<String, Object> failureWindow = (Map<String, Object>) latency.get(failureKey);
        String failure = failureWindow == null || failureWindow.isEmpty() ? "NOT_APPLICABLE" : latencyStatus(failureWindow, sloMs);

        result.put("loadAchieved", loadOk);
        result.put("correctness", correctnessStatus);
        result.put("steadyStateLatency", steady);
        result.put("failureWindowLatency", failure);

        String overall;
        if ("FAIL".equals(correctnessStatus)) {
            overall = "FAIL";
            result.put("reason", "correctness failed: the performance result is invalid");
        } else if (!loadOk) {
            overall = "INCONCLUSIVE";
            result.put("reason", "the requested offered load was not achieved");
        } else if ("INCONCLUSIVE".equals(correctnessStatus) || "INCONCLUSIVE".equals(steady) || "INCONCLUSIVE".equals(failure)) {
            overall = "INCONCLUSIVE";
            result.put("reason", "insufficient data or the run did not drain");
        } else if ("FAIL".equals(steady) || "FAIL".equals(failure)) {
            overall = "FAIL";
            result.put("reason", "latency SLO missed (correctness " + correctnessStatus + ")");
        } else {
            overall = "PASS";
        }
        result.put("overall", overall);
        return result;
    }

    @SuppressWarnings("unchecked")
    private static String latencyStatus(Map<String, Object> byKind, long sloMs) {
        boolean pass = true;
        for (String kind : List.of("INDIVIDUAL", "STAGE2", "FINAL")) {
            Map<String, Object> stats = (Map<String, Object>) byKind.get(kind);
            if (stats == null || ((Number) stats.get("count")).intValue() < MIN_SAMPLES_FOR_P9995) {
                return "INCONCLUSIVE";
            }
            if (((Number) stats.get("p99_95")).doubleValue() > sloMs) {
                pass = false;
            }
        }
        return pass ? "PASS" : "FAIL";
    }
}
