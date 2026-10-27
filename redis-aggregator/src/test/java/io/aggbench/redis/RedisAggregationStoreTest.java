package io.aggbench.redis;

import io.aggbench.domain.ActionIntent;
import io.aggbench.domain.ActionKind;
import io.aggbench.domain.ActionPlanner;
import io.aggbench.domain.AggregationLogic;
import io.aggbench.domain.AggregationState;
import io.aggbench.domain.FragmentEvent;
import io.aggbench.domain.Hashing;
import io.aggbench.domain.IngressPosition;
import io.aggbench.domain.MergeResult;
import io.aggbench.domain.fixtures.ArrivalScenarios;
import io.aggbench.domain.fixtures.ArrivalScenarios.Delivery;
import io.aggbench.domain.fixtures.ArrivalScenarios.Scenario;
import io.aggbench.redis.RedisAggregationStore.MergeReply;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the shared arrival fixtures against the real Lua function in Redis (Testcontainers, same image as the
 * Helm chart) and checks that every delivery is classified exactly like the Java state machine, and that the
 * planned intents are identical.
 */
class RedisAggregationStoreTest {

    private static GenericContainer<?> redis;
    private static RedisAggregationStore store;

    @BeforeAll
    static void startRedis() {
        redis = new GenericContainer<>("redis:7.4.6-alpine").withExposedPorts(6379);
        redis.start();
        store = new RedisAggregationStore("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379), "test",
                Duration.ofSeconds(5), Duration.ofSeconds(600), Duration.ofSeconds(120));
        store.loadFunctions();
    }

    @AfterAll
    static void stopRedis() {
        store.close();
        redis.stop();
    }

    static List<Scenario> scenarios() {
        return ArrivalScenarios.all();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("scenarios")
    void luaMatchesJavaStateMachine(Scenario scenario) throws Exception {
        String corr = "redis-" + scenario.name();
        AggregationState javaState = null;
        for (Delivery delivery : scenario.deliveries()) {
            FragmentEvent event = ArrivalScenarios.event(corr, delivery);
            IngressPosition position = ArrivalScenarios.position(delivery);
            String hash = Hashing.sha256(event.payload());

            MergeReply reply = store.merge(event, position, hash, 5_000L).get(5, TimeUnit.SECONDS);
            MergeResult java = AggregationLogic.merge(javaState, event, position, hash, 5_000L);
            javaState = java.state();

            assertEquals(delivery.expectedOutcome(), reply.outcome(), "Redis outcome for " + delivery);
            assertEquals(delivery.expectedStages(), reply.stagesDue(), "Redis stages for " + delivery);
            List<ActionIntent> fromRedis = ActionPlanner.plan(reply.outcome(), reply.stagesDue(), reply.state(), event, hash, position, 1L);
            List<ActionIntent> fromJava = ActionPlanner.plan(java.outcome(), java.stagesDue(), java.state(), event, hash, position, 1L);
            assertEquals(fromJava, fromRedis, "identical intents for " + delivery);
        }
    }

    @Test
    void concurrentMergesOfManyKeysProduceOneFinalEach() throws Exception {
        List<CompletableFuture<MergeReply>> replies = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            for (Delivery delivery : ArrivalScenarios.permutations().get(i % 6).deliveries()) {
                String corr = "bulk-" + i;
                FragmentEvent event = ArrivalScenarios.event(corr, delivery);
                replies.add(store.merge(event, new IngressPosition(i % 12, i * 3L + delivery.offset(), 1L),
                        Hashing.sha256(event.payload()), 1L));
            }
        }
        long finals = 0;
        for (CompletableFuture<MergeReply> reply : replies) {
            MergeReply r = reply.get(10, TimeUnit.SECONDS);
            if ((r.stagesDue() & 2) != 0) {
                finals++;
                assertTrue(r.state().finalEmitted());
            }
        }
        assertEquals(500, finals);
        assertEquals(0, store.openAggregates() - countOpenFromFixtures());
    }

    @Test
    void incompleteAggregatesAreTrackedAndSwept() throws Exception {
        FragmentEvent only = ArrivalScenarios.event("lonely", ArrivalScenarios.permutations().getFirst().deliveries().getFirst());
        long before = store.openAggregates();
        store.merge(only, new IngressPosition(0, 999_999, 1L), Hashing.sha256(only.payload()), 1_000L).get(5, TimeUnit.SECONDS);
        assertEquals(before + 1, store.openAggregates());
        long removed = store.sweepExpiredIncomplete(1_000L + Duration.ofSeconds(601).toMillis(), 1000);
        assertTrue(removed >= 1);
    }

    private static long countOpenFromFixtures() {
        // Incomplete fixtures from the parameterized test may still be open in the shared Redis.
        return ArrivalScenarios.all().stream().filter(s -> !s.expectComplete()).count();
    }

    @Test
    void finalIntentCarriesAllPayloads() throws Exception {
        String corr = "payload-check";
        MergeReply last = null;
        for (Delivery delivery : ArrivalScenarios.permutations().getFirst().deliveries()) {
            FragmentEvent event = ArrivalScenarios.event(corr, delivery);
            last = store.merge(event, ArrivalScenarios.position(delivery), Hashing.sha256(event.payload()), 1L).get(5, TimeUnit.SECONDS);
        }
        FragmentEvent c = ArrivalScenarios.event(corr, ArrivalScenarios.permutations().getFirst().deliveries().get(2));
        ActionIntent fin = ActionPlanner.plan(last.outcome(), last.stagesDue(), last.state(), c, Hashing.sha256(c.payload()),
                ArrivalScenarios.position(ArrivalScenarios.permutations().getFirst().deliveries().get(2)), 1L)
                .stream().filter(i -> i.kind() == ActionKind.FINAL).findFirst().orElseThrow();
        assertEquals("payload-A-v1-" + corr, fin.payloadA());
        assertEquals("payload-C-v1-" + corr, fin.payloadC());
    }
}
