package io.aggbench.redis;

import io.aggbench.execution.ActionExecutor;
import io.aggbench.execution.BenchEvents;
import io.aggbench.execution.ExecutorSettings;
import io.aggbench.execution.ManagedConsumerLoop;
import io.aggbench.execution.ProtocolVerifier;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Lifecycle of variant B: stateless pods consuming the ingress topic with the new consumer group protocol
 * ({@code group.protocol=consumer}), keeping aggregation state in Redis.
 *
 * <p>Graceful shutdown: the consumer loop stops polling, drains in-flight work (bounded), commits, and closes
 * the consumer, which leaves the group so partitions move immediately.
 */
@ApplicationScoped
public class RedisRuntime {

    @Inject
    RedisAppConfig config;

    @Inject
    MeterRegistry registry;

    private BenchEvents events;
    private RedisAggregationStore store;
    private ActionExecutor executor;
    private FragmentHandler handler;
    private ManagedConsumerLoop loop;
    private RedisHealthMonitor monitor;
    private ProtocolVerifier verifier;

    void onStart(@Observes StartupEvent event) {
        events = new BenchEvents(config.runId(), config.pod(), config.executor().persistenceUrl());
        RedisAppConfig.Redis r = config.redis();
        store = new RedisAggregationStore(r.uri(), config.runId(), r.commandTimeout(),
                config.aggregation().incompleteTtl(), config.aggregation().completedRetention());
        store.loadFunctions();

        executor = new ActionExecutor(executorSettings(), registry);
        handler = new FragmentHandler(store, executor, r.retryInitialBackoff(), r.retryMaxBackoff(), registry);
        RedisAppConfig.Consumer c = config.consumer();
        Properties properties = consumerProperties();
        loop = new ManagedConsumerLoop("ingress", properties, List.of(config.ingressTopic()), handler,
                new ManagedConsumerLoop.LoopSettings(c.maxInFlightActions(), c.commitInterval(),
                        c.revokeDrainTimeout(), c.shutdownDrainTimeout()),
                () -> executor.inFlight() + handler.pendingMerges(), events, registry);
        monitor = new RedisHealthMonitor(store, loop, events, r.replayWindow(), registry);

        Map<String, Object> staticReport = new LinkedHashMap<>();
        staticReport.put("variant", config.variant());
        staticReport.put("pod", config.pod());
        staticReport.put("runId", config.runId());
        Map<String, String> effective = new LinkedHashMap<>();
        properties.stringPropertyNames().stream().sorted().forEach(name -> effective.put(name, String.valueOf(properties.get(name))));
        staticReport.put("consumerProperties", effective);
        staticReport.put("redis", Map.of("uri", r.uri().replaceAll("//[^@]*@", "//***@"),
                "replayWindow", r.replayWindow().toString(), "redisRunId", String.valueOf(store.serverRunId())));
        staticReport.put("aggregation", Map.of("completedRetention", config.aggregation().completedRetention().toString(),
                "incompleteTtl", config.aggregation().incompleteTtl().toString()));
        events.record("INSTANCE_STARTING", staticReport);

        loop.start();
        monitor.start(r.epochCheckInterval(), config.aggregation().expiryScanInterval());
        verifier = new ProtocolVerifier(config.bootstrapServers(),
                List.of(new ProtocolVerifier.Expectation(c.groupId(), GroupType.CONSUMER, Map.of(
                        "consumer.session.timeout.ms", String.valueOf(c.sessionTimeoutMs()),
                        "consumer.heartbeat.interval.ms", String.valueOf(c.heartbeatIntervalMs())))),
                staticReport, events, () -> Quarkus.asyncExit(3), Duration.ofSeconds(180));
        verifier.start();
    }

    private Properties consumerProperties() {
        RedisAppConfig.Consumer c = config.consumer();
        if (!"consumer".equals(c.groupProtocol())) {
            throw new IllegalArgumentException("Variant B requires group.protocol=consumer, got " + c.groupProtocol());
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, c.groupId());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.pod());
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, c.groupProtocol());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, c.maxPollRecords());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }

    private ExecutorSettings executorSettings() {
        RedisAppConfig.Executor e = config.executor();
        return new ExecutorSettings(e.persistenceUrl(), e.inferenceUrl(), config.variant(), config.pod(),
                e.maxBatchSize(), e.maxConcurrentBatches(), e.maxInFlightInference(), e.requestTimeout(),
                e.inferenceTimeout(), e.retryInitialBackoff(), e.retryMaxBackoff());
    }

    void onStop(@Observes ShutdownEvent event) {
        events.record("INSTANCE_STOPPING", Map.of("inFlight", loop.inFlight()));
        verifier.close();
        monitor.close();
        loop.close();
        handler.close();
        executor.close();
        store.close();
        events.close();
    }

    boolean ready() {
        return loop != null && loop.started() && verifier.status() != ProtocolVerifier.Status.FAILED;
    }

    boolean alive() {
        return loop == null || loop.healthy();
    }

    Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("variant", config.variant());
        status.put("groupProtocol", config.consumer().groupProtocol());
        status.put("ready", ready());
        status.put("assignedPartitions", loop == null ? 0 : loop.assignedPartitions());
        status.put("inFlightRecords", loop == null ? 0 : loop.inFlight());
        status.put("redisReachable", monitor != null && monitor.reachable());
        status.put("configVerification", verifier == null ? "PENDING" : verifier.status().name());
        status.put("configReport", verifier == null ? Map.of() : verifier.report());
        return status;
    }
}
