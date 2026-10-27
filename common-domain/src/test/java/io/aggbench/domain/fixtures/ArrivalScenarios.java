package io.aggbench.domain.fixtures;

import io.aggbench.domain.FragmentEvent;
import io.aggbench.domain.FragmentType;
import io.aggbench.domain.IngressPosition;
import io.aggbench.domain.MergeOutcome;
import io.aggbench.domain.Stages;

import java.util.ArrayList;
import java.util.List;

import static io.aggbench.domain.FragmentType.A;
import static io.aggbench.domain.FragmentType.B;
import static io.aggbench.domain.FragmentType.C;
import static io.aggbench.domain.MergeOutcome.ACCEPTED;
import static io.aggbench.domain.MergeOutcome.DUPLICATE;
import static io.aggbench.domain.MergeOutcome.LATE_VERSION;
import static io.aggbench.domain.MergeOutcome.REDELIVERED;
import static io.aggbench.domain.MergeOutcome.STALE;
import static io.aggbench.domain.MergeOutcome.SUPERSEDED;

/**
 * Arrival-order fixtures with the expected classification of every delivery. The same fixtures are run
 * against the Java state machine, the Kafka Streams topology and the Redis Lua function, which is how the
 * benchmark shows that all variants implement the same business semantics.
 */
public final class ArrivalScenarios {

    private static final int NONE = 0;
    private static final int S2 = Stages.STAGE2;
    private static final int FIN = Stages.FINAL;

    /** One delivery of a fragment record; offset identifies the Kafka record. */
    public record Delivery(FragmentType type, int version, long offset, MergeOutcome expectedOutcome, int expectedStages) {
    }

    /** A named sequence of deliveries for one correlation id. */
    public record Scenario(String name, List<Delivery> deliveries, boolean expectComplete) {
        @Override
        public String toString() {
            return name;
        }
    }

    private ArrivalScenarios() {
    }

    public static List<Scenario> all() {
        List<Scenario> scenarios = new ArrayList<>(permutations());
        scenarios.add(new Scenario("duplicates-with-new-offsets", List.of(
                d(A, 1, 0, ACCEPTED, NONE),
                d(A, 1, 1, DUPLICATE, NONE),
                d(B, 1, 2, ACCEPTED, S2),
                d(B, 1, 3, DUPLICATE, NONE),
                d(C, 1, 4, ACCEPTED, FIN),
                d(C, 1, 5, DUPLICATE, NONE),
                d(A, 1, 6, DUPLICATE, NONE)), true));
        scenarios.add(new Scenario("crash-redelivery-replays-same-actions", List.of(
                d(A, 1, 10, ACCEPTED, NONE),
                d(B, 1, 11, ACCEPTED, S2),
                d(C, 1, 12, ACCEPTED, FIN),
                d(A, 1, 10, REDELIVERED, NONE),
                d(B, 1, 11, REDELIVERED, S2),
                d(C, 1, 12, REDELIVERED, FIN)), true));
        scenarios.add(new Scenario("stale-version-after-newer", List.of(
                d(A, 2, 0, ACCEPTED, NONE),
                d(A, 1, 1, STALE, NONE),
                d(B, 1, 2, ACCEPTED, S2),
                d(C, 1, 3, ACCEPTED, FIN)), true));
        scenarios.add(new Scenario("newer-version-supersedes-before-stage", List.of(
                d(A, 1, 0, ACCEPTED, NONE),
                d(A, 2, 1, SUPERSEDED, NONE),
                d(A, 1, 0, REDELIVERED, NONE),
                d(A, 1, 7, DUPLICATE, NONE),
                d(B, 1, 2, ACCEPTED, S2),
                d(C, 1, 3, ACCEPTED, FIN)), true));
        scenarios.add(new Scenario("c-superseded-after-stage2", List.of(
                d(C, 1, 0, ACCEPTED, NONE),
                d(A, 1, 1, ACCEPTED, NONE),
                d(C, 2, 2, SUPERSEDED, NONE),
                d(B, 1, 3, ACCEPTED, S2 | FIN)), true));
        scenarios.add(new Scenario("late-version-after-stage", List.of(
                d(A, 1, 0, ACCEPTED, NONE),
                d(B, 1, 1, ACCEPTED, S2),
                d(A, 2, 2, LATE_VERSION, NONE),
                d(C, 1, 3, ACCEPTED, FIN),
                d(C, 2, 4, LATE_VERSION, NONE)), true));
        scenarios.add(new Scenario("missing-b-stays-incomplete", List.of(
                d(A, 1, 0, ACCEPTED, NONE),
                d(C, 1, 1, ACCEPTED, NONE),
                d(C, 1, 2, DUPLICATE, NONE)), false));
        scenarios.add(new Scenario("missing-c-reaches-stage2-only", List.of(
                d(B, 1, 0, ACCEPTED, NONE),
                d(A, 1, 1, ACCEPTED, S2)), false));
        return scenarios;
    }

    /** All six arrival orders of A, B and C. */
    public static List<Scenario> permutations() {
        List<Scenario> result = new ArrayList<>();
        FragmentType[][] orders = {{A, B, C}, {A, C, B}, {B, A, C}, {B, C, A}, {C, A, B}, {C, B, A}};
        for (FragmentType[] order : orders) {
            List<Delivery> deliveries = new ArrayList<>();
            int mask = 0;
            boolean stage2 = false;
            for (int i = 0; i < 3; i++) {
                mask |= order[i].bit();
                int stages = 0;
                if (!stage2 && (mask & FragmentType.STAGE2_MASK) == FragmentType.STAGE2_MASK) {
                    stages |= S2;
                    stage2 = true;
                }
                if (mask == FragmentType.COMPLETE_MASK) {
                    stages |= FIN;
                }
                deliveries.add(d(order[i], 1, i, ACCEPTED, stages));
            }
            result.add(new Scenario("order-" + order[0] + order[1] + order[2], deliveries, true));
        }
        return result;
    }

    public static FragmentEvent event(String correlationId, Delivery delivery) {
        return new FragmentEvent("run-test", correlationId,
                FragmentEvent.eventId(correlationId, delivery.type(), delivery.version()),
                delivery.type().code(), delivery.version(), delivery.type() == C,
                payload(correlationId, delivery.type(), delivery.version()), 0L);
    }

    public static String payload(String correlationId, FragmentType type, int version) {
        return "payload-" + type + "-v" + version + "-" + correlationId;
    }

    public static IngressPosition position(Delivery delivery) {
        return new IngressPosition(0, delivery.offset(), 1_000L + delivery.offset());
    }

    private static Delivery d(FragmentType type, int version, long offset, MergeOutcome outcome, int stages) {
        return new Delivery(type, version, offset, outcome, stages);
    }
}
