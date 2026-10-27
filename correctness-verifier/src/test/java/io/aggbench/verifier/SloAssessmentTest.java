package io.aggbench.verifier;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SloAssessmentTest {

    private static Map<String, Object> window(double p9995, int count) {
        Map<String, Object> byKind = new HashMap<>();
        for (String kind : new String[]{"INDIVIDUAL", "STAGE2", "FINAL"}) {
            byKind.put(kind, Map.of("count", count, "p99_95", p9995));
        }
        return byKind;
    }

    private static Map<String, Object> performance(Map<String, Object> steady, Map<String, Object> failure) {
        Map<String, Object> latency = new HashMap<>();
        latency.put("steady", steady);
        latency.put("failure", failure);
        return Map.of("latency", latency);
    }

    @Test
    void passesOnlyWhenLoadCorrectnessAndBothWindowsPass() {
        Map<String, Object> result = SloAssessment.assess(Map.of("loadAchieved", true), Map.of("status", "PASS"),
                performance(window(300, 5000), window(450, 5000)), 500);
        assertEquals("PASS", result.get("overall"));
    }

    @Test
    void zeroLossButSlowRecoveryIsCorrectnessPassLatencyFail() {
        Map<String, Object> result = SloAssessment.assess(Map.of("loadAchieved", true), Map.of("status", "PASS"),
                performance(window(300, 5000), window(4000, 5000)), 500);
        assertEquals("PASS", result.get("correctness"));
        assertEquals("FAIL", result.get("failureWindowLatency"));
        assertEquals("FAIL", result.get("overall"));
    }

    @Test
    void unreachedLoadIsInconclusiveAndCorrectnessFailureInvalidates() {
        assertEquals("INCONCLUSIVE", SloAssessment.assess(Map.of("loadAchieved", false), Map.of("status", "PASS"),
                performance(window(100, 5000), Map.of()), 500).get("overall"));
        assertEquals("FAIL", SloAssessment.assess(Map.of("loadAchieved", true), Map.of("status", "FAIL"),
                performance(window(100, 5000), Map.of()), 500).get("overall"));
    }

    @Test
    void tooFewSamplesForP9995AreInconclusive() {
        Map<String, Object> result = SloAssessment.assess(Map.of("loadAchieved", true), Map.of("status", "PASS"),
                performance(window(100, 1500), Map.of()), 500);
        assertEquals("INCONCLUSIVE", result.get("steadyStateLatency"));
    }
}
