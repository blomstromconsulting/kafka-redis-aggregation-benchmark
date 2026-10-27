package io.aggbench.execution;

import java.time.Duration;

/**
 * Settings of the shared {@link ActionExecutor}. All variants use identical values (Helm values
 * {@code executor.*}), so downstream behaviour cannot favour one aggregation architecture.
 *
 * @param persistenceUrl       base URL of the persistence service
 * @param inferenceUrl         base URL of the KServe-compatible inference endpoint
 * @param variant              benchmark variant label written into receipts (a1, a2, b)
 * @param pod                  pod name written into receipts
 * @param maxBatchSize         maximum receipts/aggregates per persistence request
 * @param maxConcurrentBatches concurrent persistence requests per endpoint
 * @param maxInFlightInference concurrent inference requests
 * @param requestTimeout       persistence request timeout
 * @param inferenceTimeout     inference request timeout
 * @param retryInitialBackoff  first retry delay; doubled per attempt
 * @param retryMaxBackoff      retry delay cap (retries continue until success or shutdown)
 */
public record ExecutorSettings(
        String persistenceUrl,
        String inferenceUrl,
        String variant,
        String pod,
        int maxBatchSize,
        int maxConcurrentBatches,
        int maxInFlightInference,
        Duration requestTimeout,
        Duration inferenceTimeout,
        Duration retryInitialBackoff,
        Duration retryMaxBackoff) {

    public Duration backoff(int attempt) {
        long millis = retryInitialBackoff.toMillis() << Math.min(attempt - 1, 16);
        return Duration.ofMillis(Math.min(millis, retryMaxBackoff.toMillis()));
    }
}
