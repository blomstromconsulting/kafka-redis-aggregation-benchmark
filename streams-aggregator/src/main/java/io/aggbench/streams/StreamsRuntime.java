package io.aggbench.streams;

import io.aggbench.execution.ActionExecutor;
import io.aggbench.execution.BenchEvents;
import io.aggbench.execution.ExecutorSettings;
import io.aggbench.execution.ProtocolVerifier;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Lifecycle of the Kafka Streams variant inside Quarkus: builds the topology, starts the {@link KafkaStreams}
 * instance and the action-intent pipeline, wires recovery instrumentation, and shuts down gracefully.
 *
 * <p>Graceful shutdown (SIGTERM from Kubernetes): the Streams client is closed with
 * {@code GroupMembershipOperation.LEAVE_GROUP} so the group rebalances immediately instead of waiting for the
 * session timeout (pods are dynamic members), then in-flight actions are drained and committed.
 */
@ApplicationScoped
public class StreamsRuntime {

    private static final Logger LOG = Logger.getLogger(StreamsRuntime.class);

    @Inject
    StreamsAppConfig config;

    @Inject
    MeterRegistry registry;

    private KafkaStreams streams;
    private RecoveryInstrumentation recovery;
    private ActionIntentPipeline pipeline;
    private BenchEvents events;
    private ProtocolVerifier verifier;
    private ScheduledExecutorService poller;

    void onStart(@Observes StartupEvent event) {
        StreamsAppConfig.Streams s = config.streams();
        events = new BenchEvents(config.runId(), config.pod(), config.executor().persistenceUrl());
        BoundedRocksDbConfig.blockCacheBytes = s.rocksdbBlockCacheMb() * 1024 * 1024;
        BoundedRocksDbConfig.writeBufferBytes = s.rocksdbWriteBufferMb() * 1024 * 1024;

        StreamsAppConfig.Aggregation a = config.aggregation();
        Topology topology = AggregationTopology.build(new AggregationTopology.Settings(config.ingressTopic(),
                        config.actionsTopic(), a.completedRetention(), a.incompleteTtl(), a.expiryScanInterval(),
                        a.maxExpiriesPerScan(), StreamsConfigFactory.changelogConfig(config)),
                new ProcessorMetrics(registry, events));
        LOG.infof("Topology:%n%s", topology.describe());

        Properties properties = StreamsConfigFactory.streamsProperties(config);
        streams = new KafkaStreams(topology, properties);
        recovery = new RecoveryInstrumentation(registry, events);
        streams.setStateListener(recovery);
        streams.setGlobalStateRestoreListener(recovery);
        streams.setStandbyUpdateListener(recovery);
        streams.setUncaughtExceptionHandler(error -> {
            LOG.error("Stream thread failed; shutting down this client so Kubernetes restarts the pod", error);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        new KafkaStreamsMetrics(streams).bindTo(registry);

        ActionExecutor executor = new ActionExecutor(executorSettings(), registry);
        pipeline = new ActionIntentPipeline(config, executor, events, registry);

        Map<String, Object> staticReport = staticReport(properties);
        events.record("INSTANCE_STARTING", staticReport);
        streams.start();
        pipeline.start();

        poller = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("task-poller").daemon().factory());
        poller.scheduleAtFixedRate(() -> {
            try {
                recovery.pollTasks(streams);
            } catch (RuntimeException e) {
                LOG.debug("Task metadata poll failed", e);
            }
        }, 1, 1, TimeUnit.SECONDS);

        boolean streamsProtocol = StreamsConfigFactory.STREAMS_PROTOCOL.equals(s.groupProtocol());
        String session = String.valueOf(s.sessionTimeoutMs());
        String heartbeat = String.valueOf(s.heartbeatIntervalMs());
        verifier = new ProtocolVerifier(config.bootstrapServers(), List.of(
                new ProtocolVerifier.Expectation(s.applicationId(),
                        streamsProtocol ? GroupType.STREAMS : GroupType.CLASSIC,
                        streamsProtocol ? Map.of("streams.num.standby.replicas", String.valueOf(s.numStandbyReplicas()),
                                "streams.session.timeout.ms", session, "streams.heartbeat.interval.ms", heartbeat) : Map.of()),
                new ProtocolVerifier.Expectation(StreamsConfigFactory.intentGroupId(config), GroupType.CONSUMER,
                        Map.of("consumer.session.timeout.ms", session, "consumer.heartbeat.interval.ms", heartbeat))),
                staticReport, events, () -> Quarkus.asyncExit(3), Duration.ofSeconds(180));
        verifier.start();
    }

    private Map<String, Object> staticReport(Properties properties) {
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("variant", config.variant());
        report.put("pod", config.pod());
        report.put("runId", config.runId());
        report.put("standbyConfiguredAt", StreamsConfigFactory.STREAMS_PROTOCOL.equals(config.streams().groupProtocol())
                ? "group (streams.num.standby.replicas, set by benchmark.sh)" : "client (num.standby.replicas)");
        Map<String, String> effective = new LinkedHashMap<>();
        properties.stringPropertyNames().stream().sorted().forEach(name -> effective.put(name, String.valueOf(properties.get(name))));
        report.put("streamsProperties", effective);
        report.put("aggregation", Map.of("completedRetention", config.aggregation().completedRetention().toString(),
                "incompleteTtl", config.aggregation().incompleteTtl().toString()));
        return report;
    }

    private ExecutorSettings executorSettings() {
        StreamsAppConfig.Executor e = config.executor();
        return new ExecutorSettings(e.persistenceUrl(), e.inferenceUrl(), config.variant(), config.pod(),
                e.maxBatchSize(), e.maxConcurrentBatches(), e.maxInFlightInference(), e.requestTimeout(),
                e.inferenceTimeout(), e.retryInitialBackoff(), e.retryMaxBackoff());
    }

    void onStop(@Observes ShutdownEvent event) {
        events.record("INSTANCE_STOPPING", Map.of("state", streams.state().name()));
        poller.shutdownNow();
        verifier.close();
        boolean closed = streams.close(CloseOptions
                .groupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP)
                .withTimeout(config.streams().closeTimeout()));
        LOG.infof("Kafka Streams closed cleanly: %s", closed);
        pipeline.close();
        events.close();
    }

    KafkaStreams.State state() {
        return recovery == null ? KafkaStreams.State.CREATED : recovery.state();
    }

    boolean ready() {
        return recovery != null && recovery.reachedRunning() && pipeline.started()
                && verifier.status() != ProtocolVerifier.Status.FAILED;
    }

    boolean alive() {
        KafkaStreams.State state = state();
        return state != KafkaStreams.State.ERROR && state != KafkaStreams.State.PENDING_ERROR
                && (pipeline == null || pipeline.healthy());
    }

    Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("variant", config.variant());
        status.put("groupProtocol", config.streams().groupProtocol());
        status.put("ready", ready());
        status.put("streams", recovery == null ? Map.of() : recovery.snapshot());
        status.put("actionsInFlight", pipeline == null ? 0 : pipeline.inFlight());
        status.put("configVerification", verifier == null ? "PENDING" : verifier.status().name());
        status.put("configReport", verifier == null ? Map.of() : verifier.report());
        return status;
    }
}
