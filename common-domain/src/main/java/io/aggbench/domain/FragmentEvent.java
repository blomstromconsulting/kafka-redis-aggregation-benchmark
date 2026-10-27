package io.aggbench.domain;

/**
 * Wire format of one incoming fragment on the ingress topic (JSON value, key = correlationId).
 *
 * <p>The event id is unique per logical fragment version: {@code <correlationId>:<type>:v<version>}.
 * A duplicate delivery carries the same event id. A different version of the same fragment type has a
 * different event id and a different version number.
 *
 * @param runId             benchmark run that produced the event
 * @param correlationId     business key shared by the three fragments of one aggregate
 * @param eventId           unique id of this fragment version
 * @param eventType         1, 2 or 3 (see {@link FragmentType})
 * @param version           fragment version; higher supersedes lower
 * @param requiresInference business attribute: the complete aggregate must be scored by the ML model
 * @param payload           opaque fragment content (no overlap between fragment types)
 * @param generatedAtUs     load generator wall clock when the event was created (diagnostics only)
 */
public record FragmentEvent(
        String runId,
        String correlationId,
        String eventId,
        int eventType,
        int version,
        boolean requiresInference,
        String payload,
        long generatedAtUs) {

    public FragmentType type() {
        return FragmentType.of(eventType);
    }

    public static String eventId(String correlationId, FragmentType type, int version) {
        return correlationId + ":" + type.code() + ":v" + version;
    }
}
