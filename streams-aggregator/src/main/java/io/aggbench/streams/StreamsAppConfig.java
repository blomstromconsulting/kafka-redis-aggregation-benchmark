package io.aggbench.streams;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;

/**
 * Configuration of the Kafka Streams aggregator. A1 and A2 run this exact application; the only
 * differences are {@code bench.streams.group-protocol} and where standby replicas are configured.
 * Every value is set from Helm (see {@code helm/benchmark/templates/aggregator-streams.yaml}).
 */
@ConfigMapping(prefix = "bench")
public interface StreamsAppConfig {

    String runId();

    /** a1 or a2 (label only; behaviour is driven by {@link Streams#groupProtocol()}). */
    String variant();

    String pod();

    String ingressTopic();

    String actionsTopic();

    String bootstrapServers();

    Streams streams();

    Aggregation aggregation();

    Executor executor();

    IntentConsumer intentConsumer();

    interface Streams {
        String applicationId();

        /** "streams" (A1, KIP-1071) or "classic" (A2). */
        String groupProtocol();

        /** Standby replicas. A2: client config. A1: expected group-level config, verified at startup. */
        @WithDefault("1")
        int numStandbyReplicas();

        @WithDefault("exactly_once_v2")
        String processingGuarantee();

        @WithDefault("100")
        long commitIntervalMs();

        @WithDefault("1")
        int numStreamThreads();

        @WithDefault("/var/lib/kafka-streams")
        String stateDir();

        @WithDefault("1")
        int replicationFactor();

        /**
         * Group membership timing. A2 (classic): client configs. A1 (streams): group configs set by benchmark.sh
         * and verified at startup; the client-side values are ignored by Kafka for the streams protocol.
         */
        @WithDefault("10000")
        int sessionTimeoutMs();

        @WithDefault("1000")
        int heartbeatIntervalMs();

        @WithDefault("500")
        int maxPollRecords();

        @WithDefault("10485760")
        long cacheMaxBytes();

        @WithDefault("128")
        long rocksdbBlockCacheMb();

        @WithDefault("64")
        long rocksdbWriteBufferMb();

        @WithDefault("67108864")
        long changelogSegmentBytes();

        @WithDefault("0.3")
        String changelogMinCleanableDirtyRatio();

        @WithDefault("30s")
        Duration closeTimeout();
    }

    interface Aggregation {
        /** How long completed aggregates are kept for duplicate detection. */
        @WithDefault("120s")
        Duration completedRetention();

        /** How long an incomplete aggregate is kept before it is expired (and counted). */
        @WithDefault("600s")
        Duration incompleteTtl();

        @WithDefault("5s")
        Duration expiryScanInterval();

        @WithDefault("20000")
        int maxExpiriesPerScan();
    }

    interface Executor {
        String persistenceUrl();

        String inferenceUrl();

        @WithDefault("200")
        int maxBatchSize();

        @WithDefault("4")
        int maxConcurrentBatches();

        @WithDefault("256")
        int maxInFlightInference();

        @WithDefault("5s")
        Duration requestTimeout();

        @WithDefault("2s")
        Duration inferenceTimeout();

        @WithDefault("20ms")
        Duration retryInitialBackoff();

        @WithDefault("1s")
        Duration retryMaxBackoff();
    }

    interface IntentConsumer {
        /** Pause consumption when this many business actions are not yet durable (same limit for every variant). */
        @WithDefault("20000")
        int maxInFlightActions();

        @WithDefault("100ms")
        Duration commitInterval();

        @WithDefault("15s")
        Duration revokeDrainTimeout();

        @WithDefault("20s")
        Duration shutdownDrainTimeout();

        @WithDefault("500")
        int maxPollRecords();
    }
}
