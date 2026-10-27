package io.aggbench.streams;

import io.aggbench.domain.ActionIntent;
import io.aggbench.domain.Json;
import io.aggbench.execution.ActionExecutor;
import io.aggbench.execution.BenchEvents;
import io.aggbench.execution.ManagedConsumerLoop;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * Second half of the Streams variant: reads committed action intents (outbox) and executes them with the
 * shared {@link ActionExecutor}. Offsets of the intent topic are committed only after the action is durable,
 * so the outbox gives at-least-once execution and the idempotent persistence API makes it effectively-once.
 *
 * <p>Why an outbox: a stream thread must not block on HTTP calls, and Kafka Streams transactions cannot
 * cover external side effects. The cost is one extra Kafka hop plus waiting for the EOS commit
 * ({@code commit.interval.ms}) before the intent becomes visible to a read_committed consumer.
 */
public final class ActionIntentPipeline implements AutoCloseable {

    private final ActionExecutor executor;
    private final ManagedConsumerLoop loop;

    public ActionIntentPipeline(StreamsAppConfig config, ActionExecutor executor, BenchEvents events, MeterRegistry registry) {
        this.executor = executor;
        StreamsAppConfig.IntentConsumer c = config.intentConsumer();
        this.loop = new ManagedConsumerLoop("intents", StreamsConfigFactory.intentConsumerProperties(config),
                List.of(config.actionsTopic()), this::handle,
                new ManagedConsumerLoop.LoopSettings(c.maxInFlightActions(), c.commitInterval(),
                        c.revokeDrainTimeout(), c.shutdownDrainTimeout()),
                executor::inFlight, events, registry);
    }

    private List<CompletableFuture<?>> handle(List<ConsumerRecord<String, byte[]>> records,
                                              Predicate<ConsumerRecord<String, byte[]>> completedBefore) {
        List<CompletableFuture<?>> completions = new ArrayList<>(records.size());
        for (ConsumerRecord<String, byte[]> record : records) {
            completions.add(executor.execute(Json.fromBytes(record.value(), ActionIntent.class)));
        }
        return completions;
    }

    public void start() {
        loop.start();
    }

    public boolean started() {
        return loop.started();
    }

    public boolean healthy() {
        return loop.healthy();
    }

    public int inFlight() {
        return loop.inFlight();
    }

    @Override
    public void close() {
        loop.close();
        executor.close();
    }
}
