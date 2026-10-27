package io.aggbench.domain;

/** The three kinds of mandatory business actions. Each has an independently verifiable durable receipt. */
public enum ActionKind {
    /** Per-fragment action; one per accepted fragment event. Receipt id {@code I:<eventId>}. */
    INDIVIDUAL,
    /** Action depending on the cumulative A+B representation. Receipt id {@code S2:<correlationId>}. */
    STAGE2,
    /** Final aggregate: optional ML inference, then durable persistence. Aggregate id {@code F:<correlationId>}. */
    FINAL;

    public static String individualId(String eventId) {
        return "I:" + eventId;
    }

    public static String stage2Id(String correlationId) {
        return "S2:" + correlationId;
    }

    public static String finalId(String correlationId) {
        return "F:" + correlationId;
    }
}
