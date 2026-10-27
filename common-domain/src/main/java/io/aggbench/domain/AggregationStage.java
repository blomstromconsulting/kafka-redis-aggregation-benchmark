package io.aggbench.domain;

/** Observable lifecycle stage of an aggregate. */
public enum AggregationStage {
    /** No fragment accepted yet. */
    EMPTY,
    /** At least one fragment, but A+B not yet produced. */
    PARTIAL,
    /** A+B produced, waiting for C. */
    CUMULATIVE_AB,
    /** A+B+C produced. */
    COMPLETE
}
