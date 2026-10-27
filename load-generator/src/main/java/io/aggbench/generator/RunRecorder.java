package io.aggbench.generator;

import io.aggbench.domain.FragmentEvent;
import io.aggbench.domain.FragmentType;
import io.aggbench.domain.Hashing;
import io.aggbench.domain.Json;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Everything the load generator writes to the benchmark database: the run record, the input manifest
 * (independent ground truth for the verifier), producer and lag samples, send failures and phase events.
 * Plain JDBC on one connection; all methods are synchronized.
 */
final class RunRecorder implements AutoCloseable {

    record ProducerSample(long tsUs, String phase, double requestedRate, long sent, long acked, long failed, long maxScheduleLagMs) {
    }

    record LagSample(long tsUs, String groupId, String topic, long lag, long maxPartitionLag, long oldestAgeMs, Map<Integer, Long> perPartition) {
    }

    private final Connection connection;
    private final String runId;

    RunRecorder(String jdbcUrl, String user, String password, String runId) throws SQLException {
        this.connection = DriverManager.getConnection(jdbcUrl, user, password);
        this.connection.setAutoCommit(true);
        this.runId = runId;
    }

    synchronized void insertRun(String variant, String scenario, int repetition, Map<String, Object> config,
                                long createdUs, long plannedAggregates, long plannedEvents, double requestedRate) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("""
                INSERT INTO runs (run_id, variant, scenario, repetition, config, created_us, planned_aggregates,
                                  planned_events, requested_rate, status)
                VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, 'PLANNED')""")) {
            ps.setString(1, runId);
            ps.setString(2, variant);
            ps.setString(3, scenario);
            ps.setInt(4, repetition);
            ps.setString(5, Json.toString(config));
            ps.setLong(6, createdUs);
            ps.setLong(7, plannedAggregates);
            ps.setLong(8, plannedEvents);
            ps.setDouble(9, requestedRate);
            ps.executeUpdate();
        }
    }

    synchronized void markStarted(long startUs, long measureStartUs, long measureEndUs) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "UPDATE runs SET start_us = ?, measure_start_us = ?, measure_end_us = ?, status = 'RUNNING' WHERE run_id = ?")) {
            ps.setLong(1, startUs);
            ps.setLong(2, measureStartUs);
            ps.setLong(3, measureEndUs);
            ps.setString(4, runId);
            ps.executeUpdate();
        }
    }

    synchronized void finish(long sendEndUs, long finishedUs, long sent, long acked, long failures, double achievedRate,
                             long maxScheduleLagMs, boolean drained, String status) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("""
                UPDATE runs SET send_end_us = ?, finished_us = ?, events_sent = ?, events_acked = ?, send_failures = ?,
                                achieved_rate = ?, max_schedule_lag_ms = ?, drained = ?, status = ?
                WHERE run_id = ?""")) {
            ps.setLong(1, sendEndUs);
            ps.setLong(2, finishedUs);
            ps.setLong(3, sent);
            ps.setLong(4, acked);
            ps.setLong(5, failures);
            ps.setDouble(6, achievedRate);
            ps.setLong(7, maxScheduleLagMs);
            ps.setBoolean(8, drained);
            ps.setString(9, status);
            ps.setString(10, runId);
            ps.executeUpdate();
        }
    }

    /** Writes the manifest with COPY in chunks; returns the number of planned events. */
    synchronized long writeManifest(WorkloadPlan plan) throws Exception {
        CopyManager copy = connection.unwrap(PGConnection.class).getCopyAPI();
        String sql = "COPY manifest (run_id, correlation_id, idx, partition, start_us, warmup, sent_mask, missing_type, "
                + "duplicate_type, stale_type, out_of_order, requires_inference, hash_a, hash_b, hash_c, "
                + "expected_stage2_hash, expected_checksum) FROM STDIN WITH (FORMAT csv)";
        long events = 0;
        StringBuilder chunk = new StringBuilder(8 << 20);
        for (long i = 0; i < plan.totalAggregates(); i++) {
            WorkloadPlan.AggregatePlan p = plan.plan(i);
            events += p.events().size();
            String idA = FragmentEvent.eventId(p.correlationId(), FragmentType.A, 1);
            String idB = FragmentEvent.eventId(p.correlationId(), FragmentType.B, 1);
            String idC = FragmentEvent.eventId(p.correlationId(), FragmentType.C, 1);
            String hA = (p.sentMask() & FragmentType.A.bit()) != 0 ? plan.payloadHash(i, FragmentType.A, 1) : null;
            String hB = (p.sentMask() & FragmentType.B.bit()) != 0 ? plan.payloadHash(i, FragmentType.B, 1) : null;
            String hC = (p.sentMask() & FragmentType.C.bit()) != 0 ? plan.payloadHash(i, FragmentType.C, 1) : null;
            String stage2 = hA != null && hB != null ? Hashing.cumulativeHash(idA, hA, idB, hB) : null;
            String checksum = hA != null && hB != null && hC != null ? Hashing.aggregateChecksum(idA, hA, idB, hB, idC, hC) : null;
            chunk.append(runId).append(',').append(p.correlationId()).append(',').append(i).append(',')
                    .append(p.partition()).append(',').append(p.startUs()).append(',').append(p.warmup()).append(',')
                    .append(p.sentMask()).append(',').append(p.missingType()).append(',').append(p.duplicateType()).append(',')
                    .append(p.staleType()).append(',').append(p.outOfOrder()).append(',').append(p.requiresInference()).append(',')
                    .append(nullable(hA)).append(',').append(nullable(hB)).append(',').append(nullable(hC)).append(',')
                    .append(nullable(stage2)).append(',').append(nullable(checksum)).append('\n');
            if (chunk.length() > (6 << 20)) {
                copy.copyIn(sql, new StringReader(chunk.toString()));
                chunk.setLength(0);
            }
        }
        if (!chunk.isEmpty()) {
            copy.copyIn(sql, new StringReader(chunk.toString()));
        }
        return events;
    }

    private static String nullable(String value) {
        return value == null ? "" : value;
    }

    synchronized void insertProducerSamples(List<ProducerSample> samples) throws SQLException {
        if (samples.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = connection.prepareStatement("""
                INSERT INTO producer_samples (run_id, ts_us, phase, requested_rate, sent, acked, failed, max_schedule_lag_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""")) {
            for (ProducerSample s : samples) {
                ps.setString(1, runId);
                ps.setLong(2, s.tsUs());
                ps.setString(3, s.phase());
                ps.setDouble(4, s.requestedRate());
                ps.setLong(5, s.sent());
                ps.setLong(6, s.acked());
                ps.setLong(7, s.failed());
                ps.setLong(8, s.maxScheduleLagMs());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    synchronized void insertLagSamples(List<LagSample> samples) throws SQLException {
        if (samples.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = connection.prepareStatement("""
                INSERT INTO lag_samples (run_id, ts_us, group_id, topic, lag, max_partition_lag, oldest_age_ms, per_partition)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb)""")) {
            for (LagSample s : samples) {
                ps.setString(1, runId);
                ps.setLong(2, s.tsUs());
                ps.setString(3, s.groupId());
                ps.setString(4, s.topic());
                ps.setLong(5, s.lag());
                ps.setLong(6, s.maxPartitionLag());
                ps.setLong(7, s.oldestAgeMs());
                ps.setString(8, Json.toString(s.perPartition()));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    synchronized void insertSendFailure(String correlationId, String eventId, String error) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO send_failures (run_id, correlation_id, event_id, error) VALUES (?, ?, ?, ?)")) {
            ps.setString(1, runId);
            ps.setString(2, correlationId);
            ps.setString(3, eventId);
            ps.setString(4, error);
            ps.executeUpdate();
        }
    }

    synchronized void event(long tsUs, String type, Map<String, ?> detail) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO bench_events (run_id, ts_us, source, type, detail) VALUES (?, ?, 'load-generator', ?, ?::jsonb)")) {
            ps.setString(1, runId);
            ps.setLong(2, tsUs);
            ps.setString(3, type);
            ps.setString(4, Json.toString(detail));
            ps.executeUpdate();
        }
    }

    @Override
    public synchronized void close() throws SQLException {
        connection.close();
    }
}
