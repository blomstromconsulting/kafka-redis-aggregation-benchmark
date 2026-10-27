package io.aggbench.domain;

/** Bit flags for the cumulative stages of an aggregate. */
public final class Stages {

    /** A and B are present: the cumulative A+B representation and its dependent actions are due. */
    public static final int STAGE2 = 1;

    /** A, B and C are present: the final aggregate, ML inference and persistence are due. */
    public static final int FINAL = 2;

    private Stages() {
    }

    public static boolean has(int stages, int flag) {
        return (stages & flag) != 0;
    }
}
