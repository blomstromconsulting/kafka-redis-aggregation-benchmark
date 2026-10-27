package io.aggbench.domain;

import java.util.List;

/** Request/response bodies of the persistence service HTTP API: the contract between the action executor and persistence-service. */
public final class PersistenceApi {

    private PersistenceApi() {
    }

    /** Durable receipt of an INDIVIDUAL or STAGE2 action. Idempotency key: (runId, actionId). */
    public record Receipt(
            String runId,
            String actionId,
            String kind,
            String correlationId,
            int fragmentType,
            List<String> eventIds,
            String contentHash,
            int ingressPartition,
            long sourceAppendMs,
            long firstAppendMs,
            long computedAtUs,
            long readyAtUs,
            String variant,
            String pod) {
    }

    /** Final aggregate. Idempotency key: (runId, correlationId); version and checksum must match on retries. */
    public record AggregateRecord(
            String runId,
            String correlationId,
            int version,
            List<String> eventIds,
            String checksum,
            String payloadA,
            String payloadB,
            String payloadC,
            Double mlScore,
            String mlModel,
            long inferenceMicros,
            int ingressPartition,
            long sourceAppendMs,
            long firstAppendMs,
            long computedAtUs,
            long readyAtUs,
            String variant,
            String pod) {
    }

    /** Per-item outcome, in request order. */
    public record ItemStatus(String id, String status) {
        public static final String CREATED = "CREATED";
        public static final String DUPLICATE = "DUPLICATE";
        public static final String CONFLICT = "CONFLICT";
    }

    /** Benchmark timeline event (fault injection, assignment change, restoration, ...). */
    public record BenchEvent(String runId, long tsUs, String source, String type, String detail) {
    }
}
