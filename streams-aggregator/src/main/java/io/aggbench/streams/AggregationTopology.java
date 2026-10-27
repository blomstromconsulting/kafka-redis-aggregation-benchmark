package io.aggbench.streams;

import io.aggbench.domain.ActionIntent;
import io.aggbench.domain.AggregationState;
import io.aggbench.domain.FragmentEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Map;

/**
 * THE Kafka Streams topology, shared verbatim by A1 and A2:
 *
 * <pre>
 *   ingress topic (key = correlationId)
 *       └─► AggregationProcessor ──(aggregation-store, expiry-index; RocksDB + changelog)
 *               └─► action-intent topic (transactional outbox, read by {@link ActionIntentPipeline})
 * </pre>
 *
 * Both stores are changelogged, so a new pod with an empty {@code emptyDir} restores them from Kafka.
 * The ingress topic is keyed by correlation id, so all fragments of one aggregate land in the same task.
 */
public final class AggregationTopology {

    public static final String AGGREGATION_STORE = "aggregation-store";
    public static final String EXPIRY_INDEX_STORE = "expiry-index";
    public static final String SOURCE = "fragments";
    public static final String PROCESSOR = "aggregate";
    public static final String SINK = "action-intents";

    /**
     * @param changelogConfig topic configs applied to both changelog topics (segment size, cleaner ratio)
     */
    public record Settings(String ingressTopic, String actionsTopic, Duration completedRetention, Duration incompleteTtl,
                           Duration expiryScanInterval, int maxExpiriesPerScan, Map<String, String> changelogConfig) {
    }

    private AggregationTopology() {
    }

    public static Topology build(Settings settings, ProcessorMetrics metrics) {
        StoreBuilder<KeyValueStore<String, AggregationState>> aggregationStore = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(AGGREGATION_STORE), Serdes.String(),
                        new JsonSerde<>(AggregationState.class))
                .withLoggingEnabled(settings.changelogConfig())
                .withCachingEnabled();
        StoreBuilder<KeyValueStore<String, String>> expiryIndex = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(EXPIRY_INDEX_STORE), Serdes.String(), Serdes.String())
                .withLoggingEnabled(settings.changelogConfig())
                .withCachingEnabled();

        Topology topology = new Topology();
        topology.addSource(SOURCE, Serdes.String().deserializer(), new JsonSerde<>(FragmentEvent.class).deserializer(),
                settings.ingressTopic());
        topology.addProcessor(PROCESSOR, () -> new AggregationProcessor(settings, metrics), SOURCE);
        topology.addStateStore(aggregationStore, PROCESSOR);
        topology.addStateStore(expiryIndex, PROCESSOR);
        topology.addSink(SINK, settings.actionsTopic(), Serdes.String().serializer(),
                new JsonSerde<>(ActionIntent.class).serializer(), PROCESSOR);
        return topology;
    }
}
