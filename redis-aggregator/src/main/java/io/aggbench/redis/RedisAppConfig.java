package io.aggbench.redis;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;

/** Configuration of variant B. Executor and retention settings are identical to the Streams variant. */
@ConfigMapping(prefix = "bench")
public interface RedisAppConfig {

    String runId();

    String variant();

    String pod();

    String ingressTopic();

    String bootstrapServers();

    Consumer consumer();

    Redis redis();

    Aggregation aggregation();

    Executor executor();

    interface Consumer {
        String groupId();

        /** Must be "consumer" (KIP-848); verified against the broker at startup. */
        @WithDefault("consumer")
        String groupProtocol();

        @WithDefault("500")
        int maxPollRecords();

        /** Expected group-level session timeout (broker-side for the consumer protocol); verified at startup. */
        @WithDefault("10000")
        int sessionTimeoutMs();

        /** Expected group-level heartbeat interval (broker-side for the consumer protocol); verified at startup. */
        @WithDefault("1000")
        int heartbeatIntervalMs();

        /** Pause consumption when this many business actions are not yet durable (same limit for every variant). */
        @WithDefault("20000")
        int maxInFlightActions();

        /** Same cadence as the Streams commit interval. */
        @WithDefault("100ms")
        Duration commitInterval();

        @WithDefault("15s")
        Duration revokeDrainTimeout();

        @WithDefault("20s")
        Duration shutdownDrainTimeout();
    }

    interface Redis {
        /** redis://host:6379 or redis-sentinel://host:26379?sentinelMasterId=mymaster */
        String uri();

        @WithDefault("2s")
        Duration commandTimeout();

        @WithDefault("20ms")
        Duration retryInitialBackoff();

        @WithDefault("1s")
        Duration retryMaxBackoff();

        /** How often the server run_id is checked to detect restarts/failovers. */
        @WithDefault("500ms")
        Duration epochCheckInterval();

        /**
         * After a detected Redis restart/failover, assigned partitions are re-read from (detection time - window).
         * Must cover the longest correlation window; aggregates spanning more cannot be rebuilt.
         */
        @WithDefault("60s")
        Duration replayWindow();
    }

    interface Aggregation {
        @WithDefault("120s")
        Duration completedRetention();

        @WithDefault("600s")
        Duration incompleteTtl();

        @WithDefault("5s")
        Duration expiryScanInterval();
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
}
