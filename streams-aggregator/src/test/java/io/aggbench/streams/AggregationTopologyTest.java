package io.aggbench.streams;

import io.aggbench.domain.ActionIntent;
import io.aggbench.domain.ActionKind;
import io.aggbench.domain.FragmentEvent;
import io.aggbench.domain.MergeOutcome;
import io.aggbench.domain.Stages;
import io.aggbench.domain.fixtures.ArrivalScenarios;
import io.aggbench.domain.fixtures.ArrivalScenarios.Delivery;
import io.aggbench.domain.fixtures.ArrivalScenarios.Scenario;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Runs the shared arrival fixtures through the real topology. Crash redelivery cannot be produced by the
 * test driver (it assigns offsets itself) and is covered by exactly-once semantics in this variant, so
 * fixtures that contain REDELIVERED steps are skipped here; they run against the Java logic and Redis.
 */
class AggregationTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, FragmentEvent> input;
    private TestOutputTopic<String, ActionIntent> output;
    private Instant wallClock = Instant.parse("2026-01-01T00:00:00Z");

    @BeforeEach
    void setUp() throws Exception {
        AggregationTopology.Settings settings = new AggregationTopology.Settings("ingress", "intents",
                Duration.ofSeconds(60), Duration.ofSeconds(300), Duration.ofSeconds(5), 1000, Map.of());
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("streams-test").toString());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        driver = new TopologyTestDriver(AggregationTopology.build(settings, new ProcessorMetrics(new SimpleMeterRegistry(), null)),
                props, wallClock);
        input = driver.createInputTopic("ingress", Serdes.String().serializer(), new JsonSerde<>(FragmentEvent.class).serializer());
        output = driver.createOutputTopic("intents", Serdes.String().deserializer(), new JsonSerde<>(ActionIntent.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    static List<Scenario> scenarios() {
        return ArrivalScenarios.all().stream()
                .filter(s -> s.deliveries().stream().noneMatch(d -> d.expectedOutcome() == MergeOutcome.REDELIVERED))
                .toList();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("scenarios")
    void emitsExactlyTheExpectedIntents(Scenario scenario) {
        String corr = "corr-" + scenario.name();
        for (Delivery delivery : scenario.deliveries()) {
            input.pipeInput(corr, ArrivalScenarios.event(corr, delivery), wallClock);
            List<ActionIntent> intents = output.readValuesToList();
            long individual = intents.stream().filter(i -> i.kind() == ActionKind.INDIVIDUAL).count();
            long stage2 = intents.stream().filter(i -> i.kind() == ActionKind.STAGE2).count();
            long fin = intents.stream().filter(i -> i.kind() == ActionKind.FINAL).count();
            assertEquals(delivery.expectedOutcome().individualActionDue() ? 1 : 0, individual, "individual for " + delivery);
            assertEquals(Stages.has(delivery.expectedStages(), Stages.STAGE2) ? 1 : 0, stage2, "stage2 for " + delivery);
            assertEquals(Stages.has(delivery.expectedStages(), Stages.FINAL) ? 1 : 0, fin, "final for " + delivery);
        }
    }

    @Test
    void completedAggregatesAreEvictedAfterRetentionAndIncompleteOnesExpire() {
        for (Delivery delivery : ArrivalScenarios.permutations().getFirst().deliveries()) {
            input.pipeInput("complete", ArrivalScenarios.event("complete", delivery), wallClock);
        }
        input.pipeInput("partial", ArrivalScenarios.event("partial", ArrivalScenarios.permutations().getFirst().deliveries().getFirst()), wallClock);
        KeyValueStore<String, Object> store = driver.getKeyValueStore(AggregationTopology.AGGREGATION_STORE);
        assertNotNull(store.get("complete"));
        assertNotNull(store.get("partial"));

        driver.advanceWallClockTime(Duration.ofSeconds(61));
        assertNull(store.get("complete"), "completed aggregate evicted after completed-retention");
        assertNotNull(store.get("partial"), "incomplete aggregate kept until incomplete-ttl");

        driver.advanceWallClockTime(Duration.ofSeconds(300));
        assertNull(store.get("partial"), "incomplete aggregate expired after incomplete-ttl");
    }
}
