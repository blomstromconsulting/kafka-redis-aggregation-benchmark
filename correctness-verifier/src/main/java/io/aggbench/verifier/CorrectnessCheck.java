package io.aggbench.verifier;

import io.aggbench.domain.InferenceModel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Independent correctness verification: compares the load generator's manifest (what was sent) with the
 * durable outputs in the persistence database (what the system under test achieved). It uses no code of the
 * aggregators; only the documented hash and inference formulas from common-domain.
 */
final class CorrectnessCheck {

    private final Connection db;
    private final String runId;

    CorrectnessCheck(Connection db, String runId) {
        this.db = db;
        this.runId = runId;
    }

    Map<String, Object> run(boolean drained, long sendFailures) throws SQLException {
        Map<String, Object> result = new LinkedHashMap<>();
        Map<String, Long> finals = row("""
                SELECT count(*) FILTER (WHERE m.sent_mask = 7) AS expected,
                       count(*) FILTER (WHERE m.sent_mask = 7 AND a.correlation_id IS NOT NULL) AS observed,
                       count(*) FILTER (WHERE m.sent_mask = 7 AND a.correlation_id IS NULL) AS missing,
                       count(*) FILTER (WHERE m.sent_mask = 7 AND a.correlation_id IS NOT NULL AND (
                              a.checksum <> m.expected_checksum OR a.version <> 1
                           OR encode(sha256(convert_to(a.payload_a, 'UTF8')), 'hex') <> m.hash_a
                           OR encode(sha256(convert_to(a.payload_b, 'UTF8')), 'hex') <> m.hash_b
                           OR encode(sha256(convert_to(a.payload_c, 'UTF8')), 'hex') <> m.hash_c
                           OR a.event_ids <> m.correlation_id || ':1:v1,' || m.correlation_id || ':2:v1,' || m.correlation_id || ':3:v1'
                       )) AS wrong_content,
                       count(*) FILTER (WHERE m.sent_mask = 7 AND a.correlation_id IS NOT NULL
                                        AND m.requires_inference <> (a.ml_score IS NOT NULL)) AS inference_presence_mismatch,
                       count(*) FILTER (WHERE m.sent_mask <> 7 AND a.correlation_id IS NOT NULL) AS persisted_for_incomplete,
                       coalesce(sum(a.duplicate_attempts), 0) AS duplicate_attempts
                FROM manifest m LEFT JOIN aggregates a ON a.run_id = m.run_id AND a.correlation_id = m.correlation_id
                WHERE m.run_id = ?""", runId);
        finals.put("unknown_correlation_id", scalar("""
                SELECT count(*) FROM aggregates a WHERE a.run_id = ? AND NOT EXISTS (
                    SELECT 1 FROM manifest m WHERE m.run_id = a.run_id AND m.correlation_id = a.correlation_id)""", runId));
        finals.put("wrong_inference_score", wrongInferenceScores());
        result.put("finalAggregates", finals);

        Map<String, Long> individual = row("""
                WITH expected AS (
                    SELECT 'I:' || m.correlation_id || ':' || t.type || ':v1' AS action_id,
                           CASE t.type WHEN 1 THEN m.hash_a WHEN 2 THEN m.hash_b ELSE m.hash_c END AS hash
                    FROM manifest m CROSS JOIN (VALUES (1), (2), (3)) AS t(type)
                    WHERE m.run_id = ? AND (m.sent_mask & (1 << (t.type - 1))) <> 0)
                SELECT count(*) AS expected,
                       count(r.action_id) AS observed,
                       count(*) FILTER (WHERE r.action_id IS NULL) AS missing,
                       count(*) FILTER (WHERE r.content_hash <> e.hash) AS wrong_content,
                       coalesce(sum(r.duplicate_attempts), 0) AS duplicate_attempts
                FROM expected e LEFT JOIN action_receipts r ON r.run_id = ? AND r.action_id = e.action_id""", runId, runId);
        individual.put("unexpected", scalar("""
                SELECT count(*) FROM action_receipts r WHERE r.run_id = ? AND r.kind = 'INDIVIDUAL' AND NOT EXISTS (
                    SELECT 1 FROM manifest m WHERE m.run_id = r.run_id AND m.correlation_id = r.correlation_id
                      AND r.action_id = 'I:' || m.correlation_id || ':' || r.fragment_type || ':v1'
                      AND (m.sent_mask & (1 << (r.fragment_type - 1))) <> 0)""", runId));
        result.put("individualActions", individual);

        Map<String, Long> stage2 = row("""
                SELECT count(*) FILTER (WHERE (m.sent_mask & 3) = 3) AS expected,
                       count(*) FILTER (WHERE (m.sent_mask & 3) = 3 AND r.action_id IS NOT NULL) AS observed,
                       count(*) FILTER (WHERE (m.sent_mask & 3) = 3 AND r.action_id IS NULL) AS missing,
                       count(*) FILTER (WHERE (m.sent_mask & 3) = 3 AND r.content_hash <> m.expected_stage2_hash) AS wrong_content,
                       count(*) FILTER (WHERE (m.sent_mask & 3) <> 3 AND r.action_id IS NOT NULL) AS persisted_without_a_and_b,
                       coalesce(sum(r.duplicate_attempts), 0) AS duplicate_attempts
                FROM manifest m LEFT JOIN action_receipts r ON r.run_id = m.run_id AND r.action_id = 'S2:' || m.correlation_id
                WHERE m.run_id = ?""", runId);
        stage2.put("unknown_correlation_id", scalar("""
                SELECT count(*) FROM action_receipts r WHERE r.run_id = ? AND r.kind = 'STAGE2' AND NOT EXISTS (
                    SELECT 1 FROM manifest m WHERE m.run_id = r.run_id AND r.action_id = 'S2:' || m.correlation_id)""", runId));
        result.put("stage2Actions", stage2);

        long conflicts = scalar("SELECT count(*) FROM conflicts WHERE run_id = ?", runId);
        result.put("rejectedInconsistentDuplicates", conflicts);

        Map<String, Long> incomplete = new LinkedHashMap<>();
        try (PreparedStatement ps = db.prepareStatement(
                "SELECT sent_mask, count(*) FROM manifest WHERE run_id = ? AND sent_mask <> 7 GROUP BY sent_mask ORDER BY sent_mask")) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    incomplete.put("mask_" + rs.getInt(1), rs.getLong(2));
                }
            }
        }
        result.put("incompleteInputSequences", incomplete);
        result.put("inputVariations", row("""
                SELECT count(*) FILTER (WHERE duplicate_type <> 0) AS with_duplicate,
                       count(*) FILTER (WHERE stale_type <> 0) AS with_stale_version,
                       count(*) FILTER (WHERE out_of_order) AS out_of_order,
                       count(*) FILTER (WHERE requires_inference) AS requires_inference
                FROM manifest WHERE run_id = ?""", runId));
        result.put("sendFailures", sendFailures);

        List<String> failures = new ArrayList<>();
        check(failures, "final aggregates missing", finals.get("missing"));
        check(failures, "final aggregates with wrong content", finals.get("wrong_content"));
        check(failures, "inference presence mismatch", finals.get("inference_presence_mismatch"));
        check(failures, "wrong inference score", finals.get("wrong_inference_score"));
        check(failures, "aggregate persisted for incomplete input", finals.get("persisted_for_incomplete"));
        check(failures, "aggregate for unknown correlation id", finals.get("unknown_correlation_id"));
        check(failures, "individual actions missing", individual.get("missing"));
        check(failures, "individual actions with wrong content", individual.get("wrong_content"));
        check(failures, "unexpected individual actions", individual.get("unexpected"));
        check(failures, "stage2 actions missing", stage2.get("missing"));
        check(failures, "stage2 actions with wrong content", stage2.get("wrong_content"));
        check(failures, "stage2 action without A and B", stage2.get("persisted_without_a_and_b"));
        check(failures, "stage2 action for unknown correlation id", stage2.get("unknown_correlation_id"));
        check(failures, "inconsistent duplicate writes", conflicts);
        result.put("failures", failures);

        String status;
        if (!failures.isEmpty()) {
            status = "FAIL";
        } else if (!drained || sendFailures > 0) {
            status = "INCONCLUSIVE";
            result.put("inconclusiveReason", !drained ? "system did not drain before the timeout" : "producer send failures");
        } else {
            status = "PASS";
        }
        result.put("status", status);
        return result;
    }

    private long wrongInferenceScores() throws SQLException {
        long wrong = 0;
        db.setAutoCommit(false);
        try (PreparedStatement ps = db.prepareStatement(
                "SELECT checksum, ml_score FROM aggregates WHERE run_id = ? AND ml_score IS NOT NULL")) {
            ps.setFetchSize(10_000);
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    double expected = InferenceModel.score(InferenceModel.features(rs.getString(1)));
                    if (Math.abs(expected - rs.getDouble(2)) > 1e-6) {
                        wrong++;
                    }
                }
            }
        } finally {
            db.setAutoCommit(true);
        }
        return wrong;
    }

    private static void check(List<String> failures, String name, Long count) {
        if (count != null && count > 0) {
            failures.add(name + ": " + count);
        }
    }

    private Map<String, Long> row(String sql, Object... params) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                Map<String, Long> values = new LinkedHashMap<>();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    values.put(rs.getMetaData().getColumnLabel(i), rs.getLong(i));
                }
                return values;
            }
        }
    }

    private long scalar(String sql, Object... params) throws SQLException {
        return row(sql, params).values().iterator().next();
    }
}
