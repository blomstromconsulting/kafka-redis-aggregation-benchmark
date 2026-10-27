package io.aggbench.redis;

import io.aggbench.execution.BenchEvents;
import io.aggbench.execution.ManagedConsumerLoop;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Failure-recovery of variant B.
 *
 * <p><b>Redis loss detection.</b> Every {@code epochCheckInterval} the Redis server {@code run_id} is read.
 * It changes when Redis restarts and when Sentinel promotes a replica, i.e. exactly when acknowledged writes may
 * have been lost (Redis replication is asynchronous; AOF {@code everysec} can lose up to a second). On a change
 * the consumer loop is asked to replay all assigned partitions from {@code now - replayWindow}. Merges are
 * idempotent, so the replay rebuilds lost state without double effects.
 *
 * <p><b>Observability.</b> Also samples open (incomplete) aggregates, Redis memory, and sweeps expired incomplete
 * aggregates from the open set (counted once even with several pods sweeping).
 */
final class RedisHealthMonitor implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(RedisHealthMonitor.class);

    private final RedisAggregationStore store;
    private final ManagedConsumerLoop loop;
    private final BenchEvents events;
    private final Duration replayWindow;
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(2, Thread.ofPlatform().name("redis-monitor").daemon().factory());
    private final AtomicLong openAggregates = new AtomicLong(-1);
    private final AtomicLong usedMemory = new AtomicLong(-1);
    private final Counter epochChanges;
    private final Counter expiredIncomplete;
    private volatile String knownRunId;
    private volatile boolean reachable;

    RedisHealthMonitor(RedisAggregationStore store, ManagedConsumerLoop loop, BenchEvents events,
                       Duration replayWindow, MeterRegistry registry) {
        this.store = store;
        this.loop = loop;
        this.events = events;
        this.replayWindow = replayWindow;
        this.epochChanges = Counter.builder("bench_redis_epoch_changes_total").register(registry);
        this.expiredIncomplete = Counter.builder("bench_incomplete_expired_total").register(registry);
        registry.gauge("bench_incomplete_aggregates", openAggregates);
        registry.gauge("bench_redis_used_memory_bytes", usedMemory);
        registry.gauge("bench_redis_reachable", this, m -> m.reachable ? 1 : 0);
    }

    void start(Duration epochCheckInterval, Duration expiryScanInterval) {
        knownRunId = store.serverRunId();
        reachable = true;
        events.record("REDIS_EPOCH", Map.of("runId", knownRunId));
        scheduler.scheduleWithFixedDelay(this::checkEpoch, epochCheckInterval.toMillis(), epochCheckInterval.toMillis(), TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::sample, expiryScanInterval.toMillis(), expiryScanInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void checkEpoch() {
        try {
            String runId = store.serverRunId();
            if (!reachable) {
                events.record("REDIS_REACHABLE", Map.of("runId", String.valueOf(runId)));
            }
            reachable = true;
            if (runId != null && !Objects.equals(runId, knownRunId)) {
                epochChanges.increment();
                long replayFrom = System.currentTimeMillis() - replayWindow.toMillis();
                LOG.warnf("Redis server changed (run_id %s -> %s): possible loss of acknowledged state; replaying from %d",
                        knownRunId, runId, replayFrom);
                events.record("REDIS_EPOCH_CHANGED", Map.of("previousRunId", String.valueOf(knownRunId), "runId", runId,
                        "replayFromMs", replayFrom, "replayWindowMs", replayWindow.toMillis()));
                knownRunId = runId;
                loop.requestReplayFrom(replayFrom);
            }
        } catch (RuntimeException e) {
            if (reachable) {
                events.record("REDIS_UNREACHABLE", Map.of("error", e.toString()));
            }
            reachable = false;
        }
    }

    private void sample() {
        try {
            long expired = store.sweepExpiredIncomplete(System.currentTimeMillis(), 1000);
            if (expired > 0) {
                expiredIncomplete.increment(expired);
                events.record("INCOMPLETE_EXPIRED", Map.of("count", expired));
            }
            openAggregates.set(store.openAggregates());
            usedMemory.set(store.usedMemoryBytes());
        } catch (RuntimeException e) {
            LOG.debugf("Redis sampling failed: %s", e.toString());
        }
    }

    boolean reachable() {
        return reachable;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
