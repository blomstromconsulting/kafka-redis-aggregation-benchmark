package io.aggbench.execution;

import com.fasterxml.jackson.core.type.TypeReference;
import io.aggbench.domain.ActionIntent;
import io.aggbench.domain.ActionKind;
import io.aggbench.domain.InferenceModel;
import io.aggbench.domain.PersistenceApi.AggregateRecord;
import io.aggbench.domain.PersistenceApi.ItemStatus;
import io.aggbench.domain.PersistenceApi.Receipt;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executes business actions durably and idempotently. Shared, unchanged, by every variant.
 *
 * <ul>
 *   <li>INDIVIDUAL and STAGE2 actions become durable receipts ({@code POST /api/receipts}).</li>
 *   <li>FINAL actions run ML inference when required ({@code POST /v2/models/.../infer}) and are then
 *       persisted as the final aggregate ({@code POST /api/aggregates}).</li>
 * </ul>
 *
 * <p>Durability boundary: the future returned by {@link #execute} completes only after the persistence
 * service has committed the outcome (CREATED) or confirmed that it already exists (DUPLICATE). Callers commit
 * Kafka offsets only after that, so a crash re-executes the action and the idempotency keys absorb the repeat.
 * Retries never give up while the executor is open; on {@link #close()} pending work is cancelled and simply
 * not acknowledged.
 */
public final class ActionExecutor implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(ActionExecutor.class);
    private static final TypeReference<List<ItemStatus>> STATUS_LIST = new TypeReference<>() {
    };
    private static final TypeReference<Map<String, Object>> JSON_OBJECT = new TypeReference<>() {
    };

    private final ExecutorSettings settings;
    private final HttpJson http;
    private final ScheduledExecutorService scheduler;
    private final Batcher<Receipt> receipts;
    private final Batcher<AggregateRecord> aggregates;
    private final AsyncLimiter inferenceLimiter;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger pendingFinal = new AtomicInteger();
    private final Map<ActionKind, Timer> endToEnd = new EnumMap<>(ActionKind.class);
    private final Map<ActionKind, Timer> execution = new EnumMap<>(ActionKind.class);
    private final Map<ActionKind, Timer> aggregationReady = new EnumMap<>(ActionKind.class);
    private final Counter conflicts;
    private final Counter duplicates;
    private final Counter inferenceRetries;
    private final Timer inferenceTimer;
    private volatile boolean open = true;

    public ActionExecutor(ExecutorSettings settings, MeterRegistry registry) {
        this.settings = settings;
        this.http = new HttpJson(Duration.ofSeconds(2));
        this.scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("action-retry").daemon().factory());
        this.receipts = new Batcher<>("receipts", settings.maxBatchSize(), settings.maxConcurrentBatches(),
                batch -> http.post(settings.persistenceUrl() + "/api/receipts", batch, STATUS_LIST, settings.requestTimeout()),
                scheduler, settings, registry);
        this.aggregates = new Batcher<>("aggregates", settings.maxBatchSize(), settings.maxConcurrentBatches(),
                batch -> http.post(settings.persistenceUrl() + "/api/aggregates", batch, STATUS_LIST, settings.requestTimeout()),
                scheduler, settings, registry);
        this.inferenceLimiter = new AsyncLimiter(settings.maxInFlightInference());

        for (ActionKind kind : ActionKind.values()) {
            String k = kind.name().toLowerCase();
            endToEnd.put(kind, Timer.builder("bench_action_e2e_seconds").tag("kind", k)
                    .description("Broker append of the latest required fragment to durable completion")
                    .publishPercentileHistogram().register(registry));
            execution.put(kind, Timer.builder("bench_action_execution_seconds").tag("kind", k)
                    .description("Executor pickup to durable completion").publishPercentileHistogram().register(registry));
            aggregationReady.put(kind, Timer.builder("bench_aggregation_ready_seconds").tag("kind", k)
                    .description("Broker append of the latest required fragment to executor pickup")
                    .publishPercentileHistogram().register(registry));
        }
        this.conflicts = Counter.builder("bench_action_conflicts_total").register(registry);
        this.duplicates = Counter.builder("bench_action_duplicate_attempts_total").register(registry);
        this.inferenceRetries = Counter.builder("bench_inference_retries_total").register(registry);
        this.inferenceTimer = Timer.builder("bench_inference_seconds").publishPercentileHistogram().register(registry);
        registry.gauge("bench_actions_inflight", inFlight);
        registry.gauge("bench_pending_finalization", pendingFinal);
        registry.gauge("bench_inference_inflight", inferenceLimiter, AsyncLimiter::active);
        registry.gauge("bench_inference_queued", inferenceLimiter, AsyncLimiter::queued);
    }

    /** Executes one action; the future completes once its outcome is durable. */
    public CompletableFuture<Void> execute(ActionIntent intent) {
        if (!open) {
            return CompletableFuture.failedFuture(new CancellationException("executor closed"));
        }
        long readyAtUs = Micros.now();
        long readyNanos = System.nanoTime();
        aggregationReady.get(intent.kind()).record(Math.max(0, readyAtUs - intent.sourceAppendMs() * 1000), TimeUnit.MICROSECONDS);
        inFlight.incrementAndGet();

        CompletableFuture<String> outcome = switch (intent.kind()) {
            case INDIVIDUAL, STAGE2 -> receipts.submit(receipt(intent, readyAtUs));
            case FINAL -> {
                pendingFinal.incrementAndGet();
                long inferenceStart = System.nanoTime();
                CompletableFuture<Double> score = intent.requiresInference()
                        ? scoreWithRetry(intent, 1)
                        : CompletableFuture.completedFuture(null);
                yield score.thenCompose(value -> aggregates.submit(aggregate(intent, readyAtUs, value,
                                value == null ? 0 : (System.nanoTime() - inferenceStart) / 1000)))
                        .whenComplete((s, e) -> pendingFinal.decrementAndGet());
            }
        };

        return outcome.thenAccept(status -> {
            if (ItemStatus.CONFLICT.equals(status)) {
                conflicts.increment();
                LOG.errorf("Persistence rejected inconsistent duplicate: action=%s correlationId=%s", intent.actionId(), intent.correlationId());
            } else if (ItemStatus.DUPLICATE.equals(status)) {
                duplicates.increment();
            }
            long nowUs = Micros.now();
            endToEnd.get(intent.kind()).record(Math.max(0, nowUs - intent.sourceAppendMs() * 1000), TimeUnit.MICROSECONDS);
            execution.get(intent.kind()).record(System.nanoTime() - readyNanos, TimeUnit.NANOSECONDS);
        }).whenComplete((v, e) -> inFlight.decrementAndGet());
    }

    private CompletableFuture<Double> scoreWithRetry(ActionIntent intent, int attempt) {
        if (!open) {
            return CompletableFuture.failedFuture(new CancellationException("executor closed"));
        }
        long start = System.nanoTime();
        return inferenceLimiter.submit(() -> infer(intent))
                .thenApply(score -> {
                    inferenceTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    return score;
                })
                .exceptionallyCompose(error -> {
                    inferenceRetries.increment();
                    if (attempt % 20 == 0) {
                        LOG.warnf("Inference for %s still failing after %d attempts: %s", intent.correlationId(), attempt, error.toString());
                    }
                    CompletableFuture<Double> retry = new CompletableFuture<>();
                    scheduler.schedule(() -> scoreWithRetry(intent, attempt + 1).whenComplete((v, e) -> {
                        if (e != null) {
                            retry.completeExceptionally(e);
                        } else {
                            retry.complete(v);
                        }
                    }), settings.backoff(attempt).toMillis(), TimeUnit.MILLISECONDS);
                    return retry;
                });
    }

    /** KServe Open Inference Protocol (v2) request: the features are derived from the aggregate checksum. */
    private CompletableFuture<Double> infer(ActionIntent intent) {
        Map<String, Object> body = Map.of(
                "id", intent.actionId(),
                "inputs", List.of(Map.of(
                        "name", "features",
                        "shape", List.of(1, InferenceModel.FEATURES),
                        "datatype", "FP64",
                        "data", InferenceModel.features(intent.contentHash()))));
        String url = settings.inferenceUrl() + "/v2/models/" + InferenceModel.MODEL_NAME + "/infer";
        return http.post(url, body, JSON_OBJECT, settings.inferenceTimeout()).thenApply(ActionExecutor::firstOutput);
    }

    @SuppressWarnings("unchecked")
    private static double firstOutput(Map<String, Object> response) {
        List<Map<String, Object>> outputs = (List<Map<String, Object>>) response.get("outputs");
        List<Number> data = (List<Number>) outputs.getFirst().get("data");
        return data.getFirst().doubleValue();
    }

    private Receipt receipt(ActionIntent intent, long readyAtUs) {
        return new Receipt(intent.runId(), intent.actionId(), intent.kind().name(), intent.correlationId(),
                intent.fragmentType(), intent.eventIds(), intent.contentHash(), intent.ingressPartition(),
                intent.sourceAppendMs(), intent.firstAppendMs(), intent.computedAtUs(), readyAtUs,
                settings.variant(), settings.pod());
    }

    private AggregateRecord aggregate(ActionIntent intent, long readyAtUs, Double score, long inferenceMicros) {
        return new AggregateRecord(intent.runId(), intent.correlationId(), 1, intent.eventIds(), intent.contentHash(),
                intent.payloadA(), intent.payloadB(), intent.payloadC(), score,
                score == null ? null : InferenceModel.MODEL_NAME, inferenceMicros, intent.ingressPartition(),
                intent.sourceAppendMs(), intent.firstAppendMs(), intent.computedAtUs(), readyAtUs,
                settings.variant(), settings.pod());
    }

    public int inFlight() {
        return inFlight.get();
    }

    @Override
    public void close() {
        open = false;
        receipts.close();
        aggregates.close();
        scheduler.shutdownNow();
    }
}
