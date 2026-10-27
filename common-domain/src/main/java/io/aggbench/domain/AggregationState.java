package io.aggbench.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregation state for one correlation id, as stored by the Kafka Streams variant (RocksDB + changelog).
 * The Redis variant keeps the same fields in a Redis hash (see {@code aggregation.lua}).
 *
 * <p>Immutable: every transition returns a new instance. Transitions are defined in {@link AggregationLogic}.
 *
 * @param a, b, c           accepted fragment per type, or null
 * @param superseded        lower versions replaced before any stage consumed them (kept to recognise redeliveries)
 * @param stage2Emitted     the A+B stage has been triggered (never re-triggered)
 * @param finalEmitted      the final A+B+C stage has been triggered (never re-triggered)
 * @param createdAtMs       processing time of the first fragment
 * @param completedAtMs     processing time when the final stage was triggered, 0 while incomplete
 * @param duplicates        logical duplicates ignored
 * @param staleRejected     stale versions ignored
 * @param lateVersions      newer versions that arrived after their type was consumed by an emitted stage
 */
public record AggregationState(
        String runId,
        String correlationId,
        StoredFragment a,
        StoredFragment b,
        StoredFragment c,
        List<SupersededFragment> superseded,
        boolean stage2Emitted,
        boolean finalEmitted,
        long createdAtMs,
        long completedAtMs,
        int duplicates,
        int staleRejected,
        int lateVersions) {

    /** A replaced fragment version, remembered only so that a redelivery of its Kafka record is recognised. */
    public record SupersededFragment(String eventId, String recordRef, int triggeredStages) {
    }

    public AggregationState {
        superseded = superseded == null ? List.of() : List.copyOf(superseded);
    }

    public static AggregationState empty(String runId, String correlationId, long nowMs) {
        return new AggregationState(runId, correlationId, null, null, null, List.of(),
                false, false, nowMs, 0, 0, 0, 0);
    }

    public StoredFragment fragment(FragmentType type) {
        return switch (type) {
            case A -> a;
            case B -> b;
            case C -> c;
        };
    }

    public int mask() {
        return (a != null ? FragmentType.A.bit() : 0)
                | (b != null ? FragmentType.B.bit() : 0)
                | (c != null ? FragmentType.C.bit() : 0);
    }

    public AggregationStage stage() {
        if (finalEmitted) {
            return AggregationStage.COMPLETE;
        }
        if (stage2Emitted) {
            return AggregationStage.CUMULATIVE_AB;
        }
        return mask() == 0 ? AggregationStage.EMPTY : AggregationStage.PARTIAL;
    }

    /** True if an emitted stage already includes this fragment type, so its content is frozen. */
    public boolean consumedByEmittedStage(FragmentType type) {
        return finalEmitted || (stage2Emitted && type != FragmentType.C);
    }

    AggregationState withFragment(FragmentType type, StoredFragment fragment) {
        return new AggregationState(runId, correlationId,
                type == FragmentType.A ? fragment : a,
                type == FragmentType.B ? fragment : b,
                type == FragmentType.C ? fragment : c,
                superseded, stage2Emitted, finalEmitted, createdAtMs, completedAtMs,
                duplicates, staleRejected, lateVersions);
    }

    AggregationState withSuperseded(SupersededFragment old) {
        List<SupersededFragment> list = new ArrayList<>(superseded);
        list.add(old);
        return new AggregationState(runId, correlationId, a, b, c, list, stage2Emitted, finalEmitted,
                createdAtMs, completedAtMs, duplicates, staleRejected, lateVersions);
    }

    AggregationState withStage2Emitted() {
        return new AggregationState(runId, correlationId, a, b, c, superseded, true, finalEmitted,
                createdAtMs, completedAtMs, duplicates, staleRejected, lateVersions);
    }

    AggregationState withFinalEmitted(long nowMs) {
        return new AggregationState(runId, correlationId, a, b, c, superseded, stage2Emitted, true,
                createdAtMs, nowMs, duplicates, staleRejected, lateVersions);
    }

    AggregationState withCounters(int duplicates, int staleRejected, int lateVersions) {
        return new AggregationState(runId, correlationId, a, b, c, superseded, stage2Emitted, finalEmitted,
                createdAtMs, completedAtMs, duplicates, staleRejected, lateVersions);
    }
}
