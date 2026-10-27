package io.aggbench.execution;

import java.time.Instant;

/** Wall-clock timestamps in epoch microseconds, used for cross-component latency boundaries. */
public final class Micros {

    private Micros() {
    }

    public static long now() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000L;
    }
}
