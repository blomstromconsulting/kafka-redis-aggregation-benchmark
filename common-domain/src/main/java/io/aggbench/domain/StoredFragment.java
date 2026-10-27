package io.aggbench.domain;

/**
 * A fragment as kept in aggregation state.
 *
 * @param eventId           event id of the accepted fragment version
 * @param version           accepted version
 * @param payload           fragment content
 * @param payloadHash       SHA-256 of the payload (used in action receipts)
 * @param requiresInference copied from the event
 * @param appendTimeMs      broker append time of the accepted record
 * @param recordRef         {@code partition:offset} of the accepted record
 * @param triggeredStages   stages ({@link Stages}) that became due when this fragment was accepted;
 *                          replayed if the same Kafka record is delivered again after a crash
 */
public record StoredFragment(
        String eventId,
        int version,
        String payload,
        String payloadHash,
        boolean requiresInference,
        long appendTimeMs,
        String recordRef,
        int triggeredStages) {
}
