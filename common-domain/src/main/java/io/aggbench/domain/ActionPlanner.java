package io.aggbench.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * Turns a {@link MergeResult} into the concrete actions that are due. Shared by both variants so that the
 * same delivery always produces byte-identical intents, whichever state store was used.
 */
public final class ActionPlanner {

    private ActionPlanner() {
    }

    /**
     * @param outcome      classification of the delivery
     * @param stagesDue    stages to (re-)issue
     * @param state        aggregation state after the delivery; must contain A,B (and C) when stages are due
     * @param event        the delivered fragment
     * @param payloadHash  SHA-256 of the delivered payload
     * @param position     ingress position of the delivered record
     * @param computedAtUs time of the aggregation step
     */
    public static List<ActionIntent> plan(MergeOutcome outcome, int stagesDue, AggregationState state,
                                          FragmentEvent event, String payloadHash, IngressPosition position,
                                          long computedAtUs) {
        List<ActionIntent> intents = new ArrayList<>(3);
        if (outcome.individualActionDue()) {
            intents.add(new ActionIntent(ActionKind.individualId(event.eventId()), ActionKind.INDIVIDUAL,
                    event.runId(), event.correlationId(), event.eventType(), List.of(event.eventId()), payloadHash,
                    null, null, null, false, position.partition(),
                    position.appendTimeMs(), position.appendTimeMs(), computedAtUs));
        }
        if (Stages.has(stagesDue, Stages.STAGE2)) {
            StoredFragment a = require(state.a(), "A", state);
            StoredFragment b = require(state.b(), "B", state);
            intents.add(new ActionIntent(ActionKind.stage2Id(state.correlationId()), ActionKind.STAGE2,
                    state.runId(), state.correlationId(), 0, List.of(a.eventId(), b.eventId()),
                    Hashing.cumulativeHash(a.eventId(), a.payloadHash(), b.eventId(), b.payloadHash()),
                    a.payload(), b.payload(), null, false, position.partition(),
                    Math.max(a.appendTimeMs(), b.appendTimeMs()),
                    Math.min(a.appendTimeMs(), b.appendTimeMs()), computedAtUs));
        }
        if (Stages.has(stagesDue, Stages.FINAL)) {
            StoredFragment a = require(state.a(), "A", state);
            StoredFragment b = require(state.b(), "B", state);
            StoredFragment c = require(state.c(), "C", state);
            intents.add(new ActionIntent(ActionKind.finalId(state.correlationId()), ActionKind.FINAL,
                    state.runId(), state.correlationId(), 0, List.of(a.eventId(), b.eventId(), c.eventId()),
                    Hashing.aggregateChecksum(a.eventId(), a.payloadHash(), b.eventId(), b.payloadHash(),
                            c.eventId(), c.payloadHash()),
                    a.payload(), b.payload(), c.payload(),
                    a.requiresInference() || b.requiresInference() || c.requiresInference(),
                    position.partition(),
                    Math.max(a.appendTimeMs(), Math.max(b.appendTimeMs(), c.appendTimeMs())),
                    Math.min(a.appendTimeMs(), Math.min(b.appendTimeMs(), c.appendTimeMs())), computedAtUs));
        }
        return intents;
    }

    private static StoredFragment require(StoredFragment fragment, String name, AggregationState state) {
        if (fragment == null) {
            throw new IllegalStateException("Stage due but fragment " + name + " missing for " + state.correlationId());
        }
        return fragment;
    }
}
