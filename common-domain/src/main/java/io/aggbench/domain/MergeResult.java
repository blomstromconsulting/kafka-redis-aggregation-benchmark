package io.aggbench.domain;

/**
 * Result of applying one fragment delivery to the aggregation state.
 *
 * @param outcome      classification of the delivery
 * @param state        state after the delivery (never null)
 * @param stateChanged false when nothing needs to be written back
 * @param stagesDue    stages ({@link Stages}) whose actions must be (re-)issued for this delivery
 */
public record MergeResult(MergeOutcome outcome, AggregationState state, boolean stateChanged, int stagesDue) {
}
