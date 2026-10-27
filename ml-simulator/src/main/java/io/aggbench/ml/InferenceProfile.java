package io.aggbench.ml;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Latency/error behaviour of the simulated model server. Profiles:
 * <ul>
 *   <li>{@code fixed}: always {@code latencyMs}.</li>
 *   <li>{@code normal}: N(latencyMs, stddevMs), truncated at 0.</li>
 *   <li>{@code lognormal-tail}: log-normal around {@code latencyMs} (sigma {@code stddevMs/latencyMs}) plus, with
 *       probability {@code tailProbability}, an outlier of {@code tailLatencyMs}.</li>
 * </ul>
 * {@code slowdownFactor} multiplies every latency (used to inject "slow inference" mid-run).
 * {@code maxConcurrency} requests are served at once; up to {@code maxQueue} more wait; beyond that 503.
 */
public record InferenceProfile(
        String profile,
        double latencyMs,
        double stddevMs,
        double tailProbability,
        double tailLatencyMs,
        double errorRate,
        double timeoutRate,
        double timeoutLatencyMs,
        double slowdownFactor,
        int maxConcurrency,
        int maxQueue) {

    public long sampleLatencyMs() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        double latency = switch (profile) {
            case "fixed" -> latencyMs;
            case "normal" -> Math.max(0, latencyMs + random.nextGaussian() * stddevMs);
            case "lognormal-tail" -> {
                double sigma = latencyMs > 0 ? Math.max(0.01, stddevMs / latencyMs) : 0.01;
                double base = latencyMs * Math.exp(sigma * random.nextGaussian() - sigma * sigma / 2);
                yield random.nextDouble() < tailProbability ? tailLatencyMs : base;
            }
            default -> throw new IllegalArgumentException("Unknown inference profile " + profile);
        };
        if (timeoutRate > 0 && random.nextDouble() < timeoutRate) {
            latency = timeoutLatencyMs;
        }
        return Math.round(latency * slowdownFactor);
    }

    public boolean sampleError() {
        return errorRate > 0 && ThreadLocalRandom.current().nextDouble() < errorRate;
    }
}
