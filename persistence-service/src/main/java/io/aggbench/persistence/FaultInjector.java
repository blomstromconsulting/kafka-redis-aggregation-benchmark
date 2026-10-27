package io.aggbench.persistence;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Configurable processing latency and transient errors of the persistence service, adjustable at runtime via
 * {@code POST /admin/faults}. Two error modes matter for correctness testing:
 * <ul>
 *   <li>error-before: fail before writing (the client must retry),</li>
 *   <li>error-after-commit: write and commit, then report failure (the retry must observe DUPLICATE).</li>
 * </ul>
 */
@ApplicationScoped
public class FaultInjector {

    /** Current fault settings. */
    public record Faults(long latencyMs, long latencyJitterMs, double errorBeforeRate, double errorAfterCommitRate) {
    }

    private volatile Faults faults;

    FaultInjector(@ConfigProperty(name = "persistence.faults.latency-ms", defaultValue = "0") long latencyMs,
                  @ConfigProperty(name = "persistence.faults.latency-jitter-ms", defaultValue = "0") long jitterMs,
                  @ConfigProperty(name = "persistence.faults.error-before-rate", defaultValue = "0") double before,
                  @ConfigProperty(name = "persistence.faults.error-after-commit-rate", defaultValue = "0") double after) {
        this.faults = new Faults(latencyMs, jitterMs, before, after);
    }

    public Faults current() {
        return faults;
    }

    public void update(Faults next) {
        this.faults = next;
    }

    void simulateLatency() {
        Faults f = faults;
        long delay = f.latencyMs() + (f.latencyJitterMs() > 0 ? ThreadLocalRandom.current().nextLong(f.latencyJitterMs() + 1) : 0);
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    boolean failBeforeWrite() {
        return ThreadLocalRandom.current().nextDouble() < faults.errorBeforeRate();
    }

    boolean failAfterCommit() {
        return ThreadLocalRandom.current().nextDouble() < faults.errorAfterCommitRate();
    }
}
