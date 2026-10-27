package io.aggbench.domain;

import java.util.List;

/**
 * A business action that has become due. Produced by the aggregation step of every variant and executed
 * by the shared {@code ActionExecutor}. In the Kafka Streams variant it is also the value of the
 * transactional action-intent (outbox) topic.
 *
 * @param actionId          idempotency key ({@link ActionKind} ids)
 * @param kind              action kind
 * @param fragmentType      fragment type for INDIVIDUAL actions, 0 otherwise
 * @param eventIds          fragment event ids the action is based on (A, B, C order)
 * @param contentHash       payload hash, A+B hash or aggregate checksum (see {@link Hashing})
 * @param payloadA          A payload for STAGE2/FINAL, null for INDIVIDUAL
 * @param payloadB          B payload for STAGE2/FINAL, null for INDIVIDUAL
 * @param payloadC          C payload for FINAL only
 * @param requiresInference FINAL only: ML inference must run before persistence
 * @param ingressPartition  ingress partition of the record that made the action due
 * @param sourceAppendMs    latest broker append time among the fragments the action depends on
 * @param firstAppendMs     earliest broker append time among those fragments
 * @param computedAtUs      wall clock (µs) when the aggregation step produced this intent
 */
public record ActionIntent(
        String actionId,
        ActionKind kind,
        String runId,
        String correlationId,
        int fragmentType,
        List<String> eventIds,
        String contentHash,
        String payloadA,
        String payloadB,
        String payloadC,
        boolean requiresInference,
        int ingressPartition,
        long sourceAppendMs,
        long firstAppendMs,
        long computedAtUs) {
}
