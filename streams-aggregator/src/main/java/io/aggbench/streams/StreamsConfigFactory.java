package io.aggbench.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;
import java.util.Properties;

/**
 * All Kafka configuration of the Streams variants, kept apart from the business logic.
 *
 * <p>The ONLY intended difference between A1 and A2:
 * <ul>
 *   <li>A1: {@code group.protocol=streams}. Standby replicas, session timeout and heartbeat interval are
 *       group/broker configs ({@code streams.num.standby.replicas} set by {@code scripts/benchmark.sh} with
 *       kafka-configs.sh and verified at startup by {@link GroupProtocolVerifier}). Client-side values for these
 *       are ignored by Kafka, so they are not set.</li>
 *   <li>A2: {@code group.protocol=classic} with client-side {@code num.standby.replicas}, session timeout and
 *       heartbeat interval. The classic HighAvailabilityTaskAssignor with its default warmup settings
 *       ({@code max.warmup.replicas=2}, {@code acceptable.recovery.lag=10000},
 *       {@code probing.rebalance.interval.ms=600000}) is used unchanged.</li>
 * </ul>
 */
public final class StreamsConfigFactory {

    public static final String STREAMS_PROTOCOL = "streams";
    public static final String CLASSIC_PROTOCOL = "classic";

    private StreamsConfigFactory() {
    }

    public static Properties streamsProperties(StreamsAppConfig config) {
        StreamsAppConfig.Streams s = config.streams();
        String protocol = s.groupProtocol();
        if (!STREAMS_PROTOCOL.equals(protocol) && !CLASSIC_PROTOCOL.equals(protocol)) {
            throw new IllegalArgumentException("bench.streams.group-protocol must be 'streams' or 'classic', got " + protocol);
        }
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, s.applicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(StreamsConfig.CLIENT_ID_CONFIG, config.pod());
        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, protocol);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, s.processingGuarantee());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, s.commitIntervalMs());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, s.numStreamThreads());
        props.put(StreamsConfig.STATE_DIR_CONFIG, s.stateDir());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, s.replicationFactor());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, s.cacheMaxBytes());
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedRocksDbConfig.class);
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "INFO");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), s.maxPollRecords());
        // Streams defaults the producer linger to 100 ms; under EOS records are flushed at commit anyway.
        props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), 5);

        if (CLASSIC_PROTOCOL.equals(protocol)) {
            props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, s.numStandbyReplicas());
            props.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), s.sessionTimeoutMs());
            props.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), s.heartbeatIntervalMs());
        }
        return props;
    }

    /** Topic configs for both changelog topics so that log compaction actually runs during a benchmark. */
    public static Map<String, String> changelogConfig(StreamsAppConfig config) {
        return Map.of(
                TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(config.streams().changelogSegmentBytes()),
                TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, config.streams().changelogMinCleanableDirtyRatio());
    }

    /** The action-intent consumer: a plain consumer-protocol group reading only committed transactions. */
    public static Properties intentConsumerProperties(StreamsAppConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, intentGroupId(config));
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.pod() + "-intents");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.intentConsumer().maxPollRecords());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }

    public static String intentGroupId(StreamsAppConfig config) {
        return config.streams().applicationId() + "-intents";
    }
}
