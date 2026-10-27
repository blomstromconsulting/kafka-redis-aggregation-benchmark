package io.aggbench.domain;

import io.aggbench.domain.AggregationState.SupersededFragment;

/**
 * The aggregation state machine, as a pure function. Used directly by the Kafka Streams processor.
 * The Redis variant implements the identical rules in Lua ({@code redis-aggregator/.../aggregation.lua});
 * both are tested against the same fixtures ({@code ArrivalScenarios}).
 *
 * <p>Rules, in evaluation order, for a fragment of type T:
 * <ol>
 *   <li>No fragment of type T stored: ACCEPTED.</li>
 *   <li>Same event id as the stored (or a superseded) fragment: REDELIVERED when it is the same Kafka record
 *       (partition:offset), otherwise DUPLICATE.</li>
 *   <li>Version not higher than the stored version: STALE.</li>
 *   <li>Higher version, but an emitted stage already consumed type T: LATE_VERSION (aggregate unchanged).</li>
 *   <li>Higher version, type T not yet consumed: SUPERSEDED (the new version replaces the old one).</li>
 * </ol>
 * After ACCEPTED or SUPERSEDED, STAGE2 is triggered once when A and B are present, and FINAL once when
 * A, B and C are present. The triggered stages are remembered on the fragment so that a crash redelivery
 * of the same Kafka record re-issues exactly the same actions.
 */
public final class AggregationLogic {

    private AggregationLogic() {
    }

    public static MergeResult merge(AggregationState current, FragmentEvent event, IngressPosition position,
                                    String payloadHash, long nowMs) {
        FragmentType type = event.type();
        AggregationState state = current != null
                ? current
                : AggregationState.empty(event.runId(), event.correlationId(), nowMs);
        StoredFragment existing = state.fragment(type);

        if (existing != null) {
            if (existing.eventId().equals(event.eventId())) {
                return sameEvent(state, existing.recordRef(), existing.triggeredStages(), position);
            }
            for (SupersededFragment old : state.superseded()) {
                if (old.eventId().equals(event.eventId())) {
                    return sameEvent(state, old.recordRef(), old.triggeredStages(), position);
                }
            }
            if (event.version() <= existing.version()) {
                AggregationState next = state.withCounters(state.duplicates(), state.staleRejected() + 1, state.lateVersions());
                return new MergeResult(MergeOutcome.STALE, next, true, 0);
            }
            if (state.consumedByEmittedStage(type)) {
                AggregationState next = state.withCounters(state.duplicates(), state.staleRejected(), state.lateVersions() + 1);
                return new MergeResult(MergeOutcome.LATE_VERSION, next, true, 0);
            }
            state = state.withSuperseded(new SupersededFragment(existing.eventId(), existing.recordRef(), existing.triggeredStages()));
        }

        StoredFragment stored = new StoredFragment(event.eventId(), event.version(), event.payload(), payloadHash,
                event.requiresInference(), position.appendTimeMs(), position.recordRef(), 0);
        AggregationState next = state.withFragment(type, stored);

        int triggered = 0;
        if (!next.stage2Emitted() && (next.mask() & FragmentType.STAGE2_MASK) == FragmentType.STAGE2_MASK) {
            triggered |= Stages.STAGE2;
            next = next.withStage2Emitted();
        }
        if (!next.finalEmitted() && next.mask() == FragmentType.COMPLETE_MASK) {
            triggered |= Stages.FINAL;
            next = next.withFinalEmitted(nowMs);
        }
        if (triggered != 0) {
            next = next.withFragment(type, new StoredFragment(stored.eventId(), stored.version(), stored.payload(),
                    stored.payloadHash(), stored.requiresInference(), stored.appendTimeMs(), stored.recordRef(), triggered));
        }
        MergeOutcome outcome = existing == null ? MergeOutcome.ACCEPTED : MergeOutcome.SUPERSEDED;
        return new MergeResult(outcome, next, true, triggered);
    }

    private static MergeResult sameEvent(AggregationState state, String storedRef, int storedTriggers, IngressPosition position) {
        if (storedRef.equals(position.recordRef())) {
            return new MergeResult(MergeOutcome.REDELIVERED, state, false, storedTriggers);
        }
        AggregationState next = state.withCounters(state.duplicates() + 1, state.staleRejected(), state.lateVersions());
        return new MergeResult(MergeOutcome.DUPLICATE, next, true, 0);
    }
}
