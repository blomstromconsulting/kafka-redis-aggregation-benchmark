package io.aggbench.verifier;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StatsTest {

    @Test
    void nearestRankPercentilesAreExact() {
        long[] values = new long[10_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (values.length - i) * 1000L; // 1..10000 ms, reversed
        }
        Map<String, Object> stats = Stats.summarize(values, values.length, 500);
        assertEquals(5000.0, stats.get("p50"));
        assertEquals(9995.0, stats.get("p99_95"));
        assertEquals(9999.0, stats.get("p99_99"));
        assertEquals(10000.0, stats.get("max"));
        assertEquals(9500, stats.get("overSlo"));
    }

    @Test
    void p9995OfTwoThousandSamplesIsTheSecondLargest() {
        long[] values = new long[2000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i * 1000L;
        }
        values[1999] = 900_000_000L; // one 900 s outlier
        assertEquals(1998.0, Stats.summarize(values, 2000, 500).get("p99_95"));
    }
}
