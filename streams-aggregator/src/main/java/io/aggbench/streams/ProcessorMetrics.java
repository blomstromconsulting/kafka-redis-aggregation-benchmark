package io.aggbench.streams;

import io.aggbench.domain.ActionKind;
import io.aggbench.domain.MergeOutcome;
import io.aggbench.execution.BenchEvents;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.streams.processor.TaskId;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Metrics and timeline events emitted from inside the stream thread. */
public final class ProcessorMetrics {

    private final Map<MergeOutcome, Counter> outcomes = new EnumMap<>(MergeOutcome.class);
    private final Map<ActionKind, Counter> intents = new EnumMap<>(ActionKind.class);
    private final Map<TaskId, AtomicLong> openPerTask = new ConcurrentHashMap<>();
    private final Counter expiredIncomplete;
    private final Counter expiredCompleted;
    private final Timer processTimer;
    private final BenchEvents events;

    public ProcessorMetrics(MeterRegistry registry, BenchEvents events) {
        this.events = events;
        for (MergeOutcome outcome : MergeOutcome.values()) {
            outcomes.put(outcome, Counter.builder("bench_fragments_total").tag("outcome", outcome.name().toLowerCase()).register(registry));
        }
        for (ActionKind kind : ActionKind.values()) {
            intents.put(kind, Counter.builder("bench_intents_total").tag("kind", kind.name().toLowerCase()).register(registry));
        }
        expiredIncomplete = Counter.builder("bench_incomplete_expired_total").register(registry);
        expiredCompleted = Counter.builder("bench_completed_evicted_total").register(registry);
        processTimer = Timer.builder("bench_aggregation_step_seconds")
                .description("Time spent in the aggregation step per fragment (state read, merge, state write)")
                .publishPercentileHistogram().register(registry);
        registry.gauge("bench_incomplete_aggregates", openPerTask, m -> m.values().stream().mapToLong(AtomicLong::get).sum());
    }

    void outcome(MergeOutcome outcome) {
        outcomes.get(outcome).increment();
    }

    void intent(ActionKind kind) {
        intents.get(kind).increment();
    }

    Timer processTimer() {
        return processTimer;
    }

    AtomicLong openAggregates(TaskId task) {
        return openPerTask.computeIfAbsent(task, t -> new AtomicLong());
    }

    void taskClosed(TaskId task) {
        openPerTask.remove(task);
    }

    void expired(long incomplete, long completed) {
        expiredIncomplete.increment(incomplete);
        expiredCompleted.increment(completed);
    }

    void event(String type, Map<String, ?> detail) {
        if (events != null) {
            events.record(type, detail);
        }
    }
}
