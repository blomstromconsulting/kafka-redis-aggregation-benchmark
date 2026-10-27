package io.aggbench.domain;

import io.aggbench.domain.fixtures.ArrivalScenarios;
import io.aggbench.domain.fixtures.ArrivalScenarios.Delivery;
import io.aggbench.domain.fixtures.ArrivalScenarios.Scenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregationLogicTest {

    static List<Scenario> scenarios() {
        return ArrivalScenarios.all();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("scenarios")
    void classifiesEveryDeliveryAsExpected(Scenario scenario) {
        AggregationState state = null;
        String corr = "corr-" + scenario.name();
        for (Delivery delivery : scenario.deliveries()) {
            FragmentEvent event = ArrivalScenarios.event(corr, delivery);
            MergeResult result = AggregationLogic.merge(state, event, ArrivalScenarios.position(delivery),
                    Hashing.sha256(event.payload()), 5_000L);
            assertEquals(delivery.expectedOutcome(), result.outcome(), "outcome for " + delivery);
            assertEquals(delivery.expectedStages(), result.stagesDue(), "stages for " + delivery);
            state = result.state();
        }
        assertNotNull(state);
        assertEquals(scenario.expectComplete(), state.finalEmitted());
        assertEquals(scenario.expectComplete() ? AggregationStage.COMPLETE : state.stage(), state.stage());
    }

    @Test
    void everyArrivalOrderProducesTheSameFinalAggregate() {
        Set<String> checksums = new HashSet<>();
        Set<String> stage2Hashes = new HashSet<>();
        for (Scenario scenario : ArrivalScenarios.permutations()) {
            AggregationState state = null;
            for (Delivery delivery : scenario.deliveries()) {
                FragmentEvent event = ArrivalScenarios.event("same-key", delivery);
                String hash = Hashing.sha256(event.payload());
                MergeResult result = AggregationLogic.merge(state, event, ArrivalScenarios.position(delivery), hash, 1L);
                for (ActionIntent intent : ActionPlanner.plan(result.outcome(), result.stagesDue(), result.state(),
                        event, hash, ArrivalScenarios.position(delivery), 1L)) {
                    if (intent.kind() == ActionKind.FINAL) {
                        checksums.add(intent.contentHash());
                        assertTrue(intent.requiresInference(), "C carries requiresInference in the fixtures");
                    }
                    if (intent.kind() == ActionKind.STAGE2) {
                        stage2Hashes.add(intent.contentHash());
                    }
                }
                state = result.state();
            }
        }
        assertEquals(1, checksums.size(), "all permutations must yield one aggregate checksum");
        assertEquals(1, stage2Hashes.size(), "all permutations must yield one A+B representation");
    }

    @Test
    void plansIndividualStage2AndFinalActions() {
        String corr = "plan-key";
        AggregationState state = null;
        int individual = 0;
        int stage2 = 0;
        int fin = 0;
        for (Delivery delivery : ArrivalScenarios.permutations().getFirst().deliveries()) {
            FragmentEvent event = ArrivalScenarios.event(corr, delivery);
            String hash = Hashing.sha256(event.payload());
            IngressPosition position = ArrivalScenarios.position(delivery);
            MergeResult result = AggregationLogic.merge(state, event, position, hash, 1L);
            for (ActionIntent intent : ActionPlanner.plan(result.outcome(), result.stagesDue(), result.state(),
                    event, hash, position, 7L)) {
                switch (intent.kind()) {
                    case INDIVIDUAL -> {
                        individual++;
                        assertEquals(ActionKind.individualId(event.eventId()), intent.actionId());
                        assertEquals(hash, intent.contentHash());
                    }
                    case STAGE2 -> {
                        stage2++;
                        assertEquals(1_001L, intent.sourceAppendMs(), "latest of A(1000) and B(1001)");
                        assertEquals(1_000L, intent.firstAppendMs());
                    }
                    case FINAL -> {
                        fin++;
                        assertEquals(1_002L, intent.sourceAppendMs());
                        assertEquals(List.of(corr + ":1:v1", corr + ":2:v1", corr + ":3:v1"), intent.eventIds());
                    }
                }
            }
            state = result.state();
        }
        assertEquals(3, individual);
        assertEquals(1, stage2);
        assertEquals(1, fin);
    }

    @Test
    void duplicateAndStaleDeliveriesProduceNoActions() {
        for (MergeOutcome outcome : List.of(MergeOutcome.DUPLICATE, MergeOutcome.STALE)) {
            assertFalse(outcome.individualActionDue());
        }
    }

    @Test
    void stateRoundTripsThroughJson() {
        Delivery delivery = ArrivalScenarios.permutations().getFirst().deliveries().getFirst();
        FragmentEvent event = ArrivalScenarios.event("json-key", delivery);
        MergeResult result = AggregationLogic.merge(null, event, ArrivalScenarios.position(delivery), "h", 3L);
        AggregationState copy = Json.fromBytes(Json.toBytes(result.state()), AggregationState.class);
        assertEquals(result.state(), copy);
    }

    @Test
    void inferenceScoreIsDeterministic() {
        String checksum = Hashing.sha256("x");
        double first = InferenceModel.score(InferenceModel.features(checksum));
        double second = InferenceModel.score(InferenceModel.features(checksum));
        assertEquals(first, second);
        assertTrue(first > 0 && first < 1);
    }
}
