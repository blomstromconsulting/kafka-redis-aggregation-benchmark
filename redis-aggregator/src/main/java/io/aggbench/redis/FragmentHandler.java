package io.aggbench.redis;

import io.aggbench.domain.ActionIntent;
import io.aggbench.domain.ActionKind;
import io.aggbench.domain.ActionPlanner;
import io.aggbench.domain.FragmentEvent;
import io.aggbench.domain.Hashing;
import io.aggbench.domain.IngressPosition;
import io.aggbench.domain.Json;
import io.aggbench.domain.MergeOutcome;
import io.aggbench.execution.ActionExecutor;
import io.aggbench.execution.ManagedConsumerLoop;
import io.aggbench.execution.Micros;
import io.aggbench.redis.RedisAggregationStore.MergeReply;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * The event handler of variant B. For each ingress record:
 * <ol>
 *   <li>Atomically merge the fragment into Redis ({@link RedisAggregationStore#merge}); retried until it succeeds.</li>
 *   <li>Plan the due actions with the shared {@link ActionPlanner} (identical to the Streams variant).</li>
 *   <li>Execute them with the shared {@link ActionExecutor}.</li>
 * </ol>
 * The record's future completes when the merge and all of its actions are durable; only then can its offset be
 * committed (see {@link ManagedConsumerLoop}).
 *
 * <p>Replay after Redis data loss: records re-read below the pre-replay position already had all of their
 * work done durably, so their INDIVIDUAL action is skipped; stage actions are still executed because the rebuilt
 * state may now trigger a stage that the lost state could not (persistence absorbs repeats as DUPLICATE).
 */
final class FragmentHandler implements ManagedConsumerLoop.RecordHandler {

    private static final Logger LOG = Logger.getLogger(FragmentHandler.class);

    private final RedisAggregationStore store;
    private final ActionExecutor executor;
    private final Duration retryInitial;
    private final Duration retryMax;
    private final ScheduledExecutorService retryScheduler =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("redis-retry").daemon().factory());
    private final Timer mergeTimer;
    private final Counter mergeRetries;
    private final Counter replayedRecords;
    private final Map<MergeOutcome, Counter> outcomes = new EnumMap<>(MergeOutcome.class);
    private final Map<ActionKind, Counter> intents = new EnumMap<>(ActionKind.class);
    private final java.util.concurrent.atomic.AtomicInteger pendingMerges = new java.util.concurrent.atomic.AtomicInteger();
    private volatile boolean open = true;

    FragmentHandler(RedisAggregationStore store, ActionExecutor executor, Duration retryInitial, Duration retryMax,
                    MeterRegistry registry) {
        this.store = store;
        this.executor = executor;
        this.retryInitial = retryInitial;
        this.retryMax = retryMax;
        this.mergeTimer = Timer.builder("bench_redis_merge_seconds")
                .description("Redis FCALL aggbench_merge round trip, including pipelining queue time")
                .publishPercentileHistogram().register(registry);
        this.mergeRetries = Counter.builder("bench_redis_merge_retries_total").register(registry);
        this.replayedRecords = Counter.builder("bench_redis_replayed_records_total").register(registry);
        for (MergeOutcome outcome : MergeOutcome.values()) {
            outcomes.put(outcome, Counter.builder("bench_fragments_total").tag("outcome", outcome.name().toLowerCase()).register(registry));
        }
        for (ActionKind kind : ActionKind.values()) {
            intents.put(kind, Counter.builder("bench_intents_total").tag("kind", kind.name().toLowerCase()).register(registry));
        }
    }

    @Override
    public List<CompletableFuture<?>> handle(List<ConsumerRecord<String, byte[]>> records,
                                             Predicate<ConsumerRecord<String, byte[]>> completedBefore) {
        List<CompletableFuture<?>> completions = new ArrayList<>(records.size());
        for (ConsumerRecord<String, byte[]> record : records) {
            FragmentEvent event = Json.fromBytes(record.value(), FragmentEvent.class);
            IngressPosition position = new IngressPosition(record.partition(), record.offset(), record.timestamp());
            String payloadHash = Hashing.sha256(event.payload());
            boolean replay = completedBefore.test(record);
            if (replay) {
                replayedRecords.increment();
            }
            pendingMerges.incrementAndGet();
            completions.add(mergeWithRetry(event, position, payloadHash, 1)
                    .whenComplete((reply, error) -> pendingMerges.decrementAndGet())
                    .thenCompose(reply -> executeActions(reply, event, position, payloadHash, replay)));
        }
        return completions;
    }

    private CompletableFuture<MergeReply> mergeWithRetry(FragmentEvent event, IngressPosition position,
                                                         String payloadHash, int attempt) {
        long start = System.nanoTime();
        return store.merge(event, position, payloadHash, System.currentTimeMillis())
                .thenApply(reply -> {
                    mergeTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    return reply;
                })
                .exceptionallyCompose(error -> {
                    if (!open) {
                        return CompletableFuture.failedFuture(error);
                    }
                    mergeRetries.increment();
                    if (attempt % 20 == 1) {
                        LOG.warnf("Redis merge failed (attempt %d) for %s: %s", attempt, event.eventId(), error.toString());
                    }
                    long delay = Math.min(retryMax.toMillis(), retryInitial.toMillis() << Math.min(attempt - 1, 16));
                    CompletableFuture<MergeReply> retry = new CompletableFuture<>();
                    retryScheduler.schedule(() -> mergeWithRetry(event, position, payloadHash, attempt + 1)
                            .whenComplete((value, e) -> {
                                if (e != null) {
                                    retry.completeExceptionally(e);
                                } else {
                                    retry.complete(value);
                                }
                            }), delay, TimeUnit.MILLISECONDS);
                    return retry;
                });
    }

    private CompletableFuture<Void> executeActions(MergeReply reply, FragmentEvent event, IngressPosition position,
                                                   String payloadHash, boolean replay) {
        outcomes.get(reply.outcome()).increment();
        List<ActionIntent> planned = ActionPlanner.plan(reply.outcome(), reply.stagesDue(), reply.state(), event,
                payloadHash, position, Micros.now());
        List<CompletableFuture<Void>> running = new ArrayList<>(planned.size());
        for (ActionIntent intent : planned) {
            if (replay && intent.kind() == ActionKind.INDIVIDUAL) {
                continue;
            }
            intents.get(intent.kind()).increment();
            running.add(executor.execute(intent));
        }
        return running.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.allOf(running.toArray(CompletableFuture[]::new));
    }

    /** Fragments whose Redis merge has not completed yet (counted as in-flight work for back-pressure). */
    int pendingMerges() {
        return pendingMerges.get();
    }

    void close() {
        open = false;
        retryScheduler.shutdownNow();
    }
}
