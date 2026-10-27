package io.aggbench.verifier;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/** Exact latency statistics from raw samples (no histogram buckets, no averaged percentiles). */
final class Stats {

    static final double[] QUANTILES = {0.50, 0.95, 0.99, 0.999, 0.9995, 0.9999};
    static final String[] NAMES = {"p50", "p95", "p99", "p99_9", "p99_95", "p99_99"};

    private Stats() {
    }

    /** Latencies in microseconds; result in milliseconds. Uses the nearest-rank (percentile_disc) definition. */
    static Map<String, Object> summarize(long[] latenciesUs, int count, long sloMs) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("count", count);
        if (count == 0) {
            return result;
        }
        long[] sorted = Arrays.copyOf(latenciesUs, count);
        Arrays.sort(sorted);
        for (int i = 0; i < QUANTILES.length; i++) {
            result.put(NAMES[i], ms(sorted[rank(QUANTILES[i], count)]));
        }
        result.put("max", ms(sorted[count - 1]));
        long sloUs = sloMs * 1000;
        int over = count - upperBound(sorted, sloUs);
        result.put("overSlo", over);
        result.put("overSloFraction", (double) over / count);
        return result;
    }

    static int rank(double quantile, int count) {
        return Math.min(count - 1, Math.max(0, (int) Math.ceil(quantile * count) - 1));
    }

    static double ms(long micros) {
        return Math.round(micros / 10.0) / 100.0;
    }

    /** Index of the first element greater than {@code value}. */
    private static int upperBound(long[] sorted, long value) {
        int lo = 0;
        int hi = sorted.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (sorted[mid] <= value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /** Growable primitive list. */
    static final class LongList {
        long[] values = new long[1024];
        int size;

        void add(long value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, size * 2);
            }
            values[size++] = value;
        }
    }
}
