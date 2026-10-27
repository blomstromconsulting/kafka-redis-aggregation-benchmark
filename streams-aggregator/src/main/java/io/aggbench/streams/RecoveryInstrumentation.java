package io.aggbench.streams;

import io.aggbench.execution.BenchEvents;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.processor.StandbyUpdateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Failure-recovery instrumentation for A1/A2:
 * <ul>
 *   <li>Active-task restoration from the changelog (start, records restored, end, duration).</li>
 *   <li>Standby updates and standby lag (how far a standby is behind its changelog end: "standby readiness").</li>
 *   <li>Client state transitions (CREATED → REBALANCING ↔ RUNNING ...).</li>
 *   <li>Active/standby task sets, polled every second, with a timeline event on every change.</li>
 * </ul>
 * Everything is exported as Prometheus metrics and as benchmark timeline events.
 */
public final class RecoveryInstrumentation implements StateRestoreListener, StandbyUpdateListener, KafkaStreams.StateListener {

    private final BenchEvents events;
    private final Map<TopicPartition, Long> restoreStartedAt = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Long> standbyLag = new ConcurrentHashMap<>();
    private final Counter restoredRecords;
    private final Counter standbyRecords;
    private final Timer restoreDuration;
    private volatile KafkaStreams.State state = KafkaStreams.State.CREATED;
    private volatile boolean reachedRunning;
    private volatile Set<String> activeTasks = Set.of();
    private volatile Set<String> standbyTasks = Set.of();

    public RecoveryInstrumentation(MeterRegistry registry, BenchEvents events) {
        this.events = events;
        this.restoredRecords = Counter.builder("bench_streams_restored_records_total").register(registry);
        this.standbyRecords = Counter.builder("bench_streams_standby_records_total").register(registry);
        this.restoreDuration = Timer.builder("bench_streams_restore_seconds").publishPercentileHistogram().register(registry);
        registry.gauge("bench_streams_state", this, i -> i.state.ordinal());
        registry.gauge("bench_streams_running", this, i -> i.state == KafkaStreams.State.RUNNING ? 1 : 0);
        registry.gauge("bench_streams_active_tasks", this, i -> i.activeTasks.size());
        registry.gauge("bench_streams_standby_tasks", this, i -> i.standbyTasks.size());
        registry.gauge("bench_streams_restoring_partitions", restoreStartedAt, Map::size);
        registry.gauge("bench_streams_standby_lag_records", standbyLag,
                m -> m.values().stream().mapToLong(Long::longValue).max().orElse(0));
    }

    // ---- active task restoration -------------------------------------------------------------

    @Override
    public void onRestoreStart(TopicPartition partition, String storeName, long startingOffset, long endingOffset) {
        restoreStartedAt.put(partition, System.nanoTime());
        events.record("RESTORE_START", Map.of("changelog", partition.toString(), "store", storeName,
                "startOffset", startingOffset, "endOffset", endingOffset, "recordsToRestore", endingOffset - startingOffset));
    }

    @Override
    public void onBatchRestored(TopicPartition partition, String storeName, long batchEndOffset, long numRestored) {
        restoredRecords.increment(numRestored);
    }

    @Override
    public void onRestoreEnd(TopicPartition partition, String storeName, long totalRestored) {
        Long start = restoreStartedAt.remove(partition);
        long durationMs = start == null ? -1 : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        if (start != null) {
            restoreDuration.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
        events.record("RESTORE_END", Map.of("changelog", partition.toString(), "store", storeName,
                "recordsRestored", totalRestored, "durationMs", durationMs));
    }

    @Override
    public void onRestoreSuspended(TopicPartition partition, String storeName, long totalRestored) {
        restoreStartedAt.remove(partition);
        events.record("RESTORE_SUSPENDED", Map.of("changelog", partition.toString(), "store", storeName,
                "recordsRestored", totalRestored));
    }

    // ---- standby tasks ------------------------------------------------------------------------

    @Override
    public void onUpdateStart(TopicPartition partition, String storeName, long startingOffset) {
        events.record("STANDBY_UPDATE_START", Map.of("changelog", partition.toString(), "store", storeName,
                "startOffset", startingOffset));
    }

    @Override
    public void onBatchLoaded(TopicPartition partition, String storeName, TaskId taskId, long batchEndOffset,
                              long batchSize, long currentEndOffset) {
        standbyRecords.increment(batchSize);
        standbyLag.put(partition, Math.max(0, currentEndOffset - batchEndOffset));
    }

    @Override
    public void onUpdateSuspended(TopicPartition partition, String storeName, long storeOffset, long currentEndOffset,
                                  SuspendReason reason) {
        standbyLag.remove(partition);
        events.record("STANDBY_UPDATE_SUSPENDED", Map.of("changelog", partition.toString(), "store", storeName,
                "reason", reason.name(), "lagAtSuspend", currentEndOffset - storeOffset));
    }

    // ---- client state and task assignment ------------------------------------------------------

    @Override
    public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        state = newState;
        if (newState == KafkaStreams.State.RUNNING) {
            reachedRunning = true;
        }
        events.record("STREAMS_STATE", Map.of("from", oldState.name(), "to", newState.name()));
    }

    void pollTasks(KafkaStreams streams) {
        Set<String> active = new TreeSet<>();
        Set<String> standby = new TreeSet<>();
        for (ThreadMetadata thread : streams.metadataForLocalThreads()) {
            thread.activeTasks().stream().map(TaskMetadata::taskId).map(TaskId::toString).forEach(active::add);
            thread.standbyTasks().stream().map(TaskMetadata::taskId).map(TaskId::toString).forEach(standby::add);
        }
        if (!active.equals(activeTasks) || !standby.equals(standbyTasks)) {
            Map<String, Object> detail = new HashMap<>();
            detail.put("active", active);
            detail.put("standby", standby);
            detail.put("activeAdded", difference(active, activeTasks));
            detail.put("activeRemoved", difference(activeTasks, active));
            detail.put("standbyAdded", difference(standby, standbyTasks));
            detail.put("standbyRemoved", difference(standbyTasks, standby));
            activeTasks = active;
            standbyTasks = standby;
            events.record("TASKS_CHANGED", detail);
        }
    }

    private static Set<String> difference(Set<String> a, Set<String> b) {
        Set<String> result = new HashSet<>(a);
        result.removeAll(b);
        return result;
    }

    KafkaStreams.State state() {
        return state;
    }

    boolean reachedRunning() {
        return reachedRunning;
    }

    Map<String, Object> snapshot() {
        Map<String, Object> snapshot = new HashMap<>();
        snapshot.put("state", state.name());
        snapshot.put("reachedRunning", reachedRunning);
        snapshot.put("activeTasks", activeTasks);
        snapshot.put("standbyTasks", standbyTasks);
        snapshot.put("restoringChangelogPartitions", restoreStartedAt.keySet().stream().map(TopicPartition::toString).sorted().toList());
        Map<String, Long> lag = new HashMap<>();
        standbyLag.forEach((partition, value) -> lag.put(partition.toString(), value));
        snapshot.put("standbyLagByChangelogPartition", lag);
        return snapshot;
    }
}
