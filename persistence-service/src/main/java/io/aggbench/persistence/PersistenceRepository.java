package io.aggbench.persistence;

import io.agroal.api.AgroalDataSource;
import io.aggbench.domain.PersistenceApi.AggregateRecord;
import io.aggbench.domain.PersistenceApi.BenchEvent;
import io.aggbench.domain.PersistenceApi.ItemStatus;
import io.aggbench.domain.PersistenceApi.Receipt;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * Idempotent writes with plain JDBC. Each batch is one transaction:
 * <pre>
 *   INSERT ... SELECT FROM unnest(arrays) ON CONFLICT (key) DO UPDATE SET duplicate_attempts = +1
 *   RETURNING key, (xmax = 0) AS inserted, stored hash
 * </pre>
 * {@code inserted} distinguishes CREATED from DUPLICATE; a duplicate whose hash differs from the stored one is a
 * CONFLICT, recorded in {@code conflicts} and never overwrites the stored row. After COMMIT returns, the commit
 * time of the batch is recorded in {@code commit_batches}; latency analysis uses it as the durable-completion time.
 */
@ApplicationScoped
public class PersistenceRepository {

    private static final String INSERT_RECEIPTS = """
            INSERT INTO action_receipts (run_id, action_id, kind, correlation_id, fragment_type, event_ids, content_hash,
                ingress_partition, source_append_us, first_append_us, computed_us, ready_us, precommit_us, batch_id, variant, pod)
            SELECT * FROM unnest(?::text[], ?::text[], ?::text[], ?::text[], ?::int[], ?::text[], ?::text[],
                ?::int[], ?::bigint[], ?::bigint[], ?::bigint[], ?::bigint[], ?::bigint[], ?::bigint[], ?::text[], ?::text[])
            ON CONFLICT (run_id, action_id) DO UPDATE SET duplicate_attempts = action_receipts.duplicate_attempts + 1
            RETURNING action_id, (xmax = 0) AS inserted, content_hash
            """;

    private static final String INSERT_AGGREGATES = """
            INSERT INTO aggregates (run_id, correlation_id, version, event_ids, checksum, payload_a, payload_b, payload_c,
                ml_score, ml_model, inference_us, ingress_partition, source_append_us, first_append_us, computed_us,
                ready_us, precommit_us, batch_id, variant, pod)
            SELECT * FROM unnest(?::text[], ?::text[], ?::int[], ?::text[], ?::text[], ?::text[], ?::text[], ?::text[],
                ?::float8[], ?::text[], ?::bigint[], ?::int[], ?::bigint[], ?::bigint[], ?::bigint[],
                ?::bigint[], ?::bigint[], ?::bigint[], ?::text[], ?::text[])
            ON CONFLICT (run_id, correlation_id) DO UPDATE SET duplicate_attempts = aggregates.duplicate_attempts + 1
            RETURNING correlation_id, (xmax = 0) AS inserted, checksum || '/v' || version
            """;

    @Inject
    AgroalDataSource dataSource;

    @Inject
    FaultInjector faults;

    public List<ItemStatus> insertReceipts(List<Receipt> items) throws SQLException {
        return write(items, Receipt::actionId, Receipt::contentHash, Receipt::runId, "receipt", (connection, unique, batchId, precommitUs) -> {
            try (PreparedStatement ps = connection.prepareStatement(INSERT_RECEIPTS)) {
                int i = 1;
                ps.setArray(i++, texts(connection, unique, Receipt::runId));
                ps.setArray(i++, texts(connection, unique, Receipt::actionId));
                ps.setArray(i++, texts(connection, unique, Receipt::kind));
                ps.setArray(i++, texts(connection, unique, Receipt::correlationId));
                ps.setArray(i++, ints(connection, unique, Receipt::fragmentType));
                ps.setArray(i++, texts(connection, unique, r -> String.join(",", r.eventIds())));
                ps.setArray(i++, texts(connection, unique, Receipt::contentHash));
                ps.setArray(i++, ints(connection, unique, Receipt::ingressPartition));
                ps.setArray(i++, longs(connection, unique, r -> r.sourceAppendMs() * 1000));
                ps.setArray(i++, longs(connection, unique, r -> r.firstAppendMs() * 1000));
                ps.setArray(i++, longs(connection, unique, Receipt::computedAtUs));
                ps.setArray(i++, longs(connection, unique, Receipt::readyAtUs));
                ps.setArray(i++, longs(connection, unique, r -> precommitUs));
                ps.setArray(i++, longs(connection, unique, r -> batchId));
                ps.setArray(i++, texts(connection, unique, Receipt::variant));
                ps.setArray(i, texts(connection, unique, Receipt::pod));
                return results(ps);
            }
        });
    }

    public List<ItemStatus> insertAggregates(List<AggregateRecord> items) throws SQLException {
        return write(items, AggregateRecord::correlationId, a -> a.checksum() + "/v" + a.version(), AggregateRecord::runId, "aggregate",
                (connection, unique, batchId, precommitUs) -> {
                    try (PreparedStatement ps = connection.prepareStatement(INSERT_AGGREGATES)) {
                        int i = 1;
                        ps.setArray(i++, texts(connection, unique, AggregateRecord::runId));
                        ps.setArray(i++, texts(connection, unique, AggregateRecord::correlationId));
                        ps.setArray(i++, ints(connection, unique, AggregateRecord::version));
                        ps.setArray(i++, texts(connection, unique, a -> String.join(",", a.eventIds())));
                        ps.setArray(i++, texts(connection, unique, AggregateRecord::checksum));
                        ps.setArray(i++, texts(connection, unique, AggregateRecord::payloadA));
                        ps.setArray(i++, texts(connection, unique, AggregateRecord::payloadB));
                        ps.setArray(i++, texts(connection, unique, AggregateRecord::payloadC));
                        ps.setArray(i++, connection.createArrayOf("float8", unique.stream().map(AggregateRecord::mlScore).toArray()));
                        ps.setArray(i++, texts(connection, unique, AggregateRecord::mlModel));
                        ps.setArray(i++, longs(connection, unique, AggregateRecord::inferenceMicros));
                        ps.setArray(i++, ints(connection, unique, AggregateRecord::ingressPartition));
                        ps.setArray(i++, longs(connection, unique, a -> a.sourceAppendMs() * 1000));
                        ps.setArray(i++, longs(connection, unique, a -> a.firstAppendMs() * 1000));
                        ps.setArray(i++, longs(connection, unique, AggregateRecord::computedAtUs));
                        ps.setArray(i++, longs(connection, unique, AggregateRecord::readyAtUs));
                        ps.setArray(i++, longs(connection, unique, a -> precommitUs));
                        ps.setArray(i++, longs(connection, unique, a -> batchId));
                        ps.setArray(i++, texts(connection, unique, AggregateRecord::variant));
                        ps.setArray(i, texts(connection, unique, AggregateRecord::pod));
                        return results(ps);
                    }
                });
    }

    /** Writes one batch with the given statement; maps per-row results back to the request order. */
    private <T> List<ItemStatus> write(List<T> items, Function<T, String> id, Function<T, String> hash,
                                       Function<T, String> runId, String kind, BatchInsert<T> insert) throws SQLException {
        // The same key twice in one statement is not allowed by ON CONFLICT DO UPDATE: keep the first occurrence.
        Map<String, T> unique = new LinkedHashMap<>();
        for (T item : items) {
            unique.putIfAbsent(id.apply(item), item);
        }
        List<T> uniqueItems = new ArrayList<>(unique.values());
        long batchId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        Map<String, StoredRow> stored;
        long precommitUs;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            precommitUs = nowMicros();
            stored = insert.execute(connection, uniqueItems, batchId, precommitUs);
            List<String[]> conflicts = new ArrayList<>();
            for (T item : uniqueItems) {
                StoredRow row = stored.get(id.apply(item));
                if (row != null && !row.inserted() && !row.hash().equals(hash.apply(item))) {
                    conflicts.add(new String[]{runId.apply(item), id.apply(item), row.hash(), hash.apply(item)});
                }
            }
            recordConflicts(connection, kind, conflicts);
            connection.commit();
        }
        long postcommitUs = nowMicros();
        recordCommit(batchId, runId.apply(items.getFirst()), uniqueItems.size(), precommitUs, postcommitUs);

        Map<String, Boolean> firstSeen = new HashMap<>();
        List<ItemStatus> statuses = new ArrayList<>(items.size());
        for (T item : items) {
            String key = id.apply(item);
            StoredRow row = stored.get(key);
            String status;
            if (row == null) {
                throw new SQLException("No result row for " + key);
            } else if (!row.hash().equals(hash.apply(item))) {
                status = ItemStatus.CONFLICT;
            } else if (row.inserted() && firstSeen.putIfAbsent(key, Boolean.TRUE) == null) {
                status = ItemStatus.CREATED;
            } else {
                status = ItemStatus.DUPLICATE;
            }
            statuses.add(new ItemStatus(key, status));
        }
        return statuses;
    }

    private record StoredRow(boolean inserted, String hash) {
    }

    @FunctionalInterface
    private interface BatchInsert<T> {
        Map<String, StoredRow> execute(Connection connection, List<T> items, long batchId, long precommitUs) throws SQLException;
    }

    private static Map<String, StoredRow> results(PreparedStatement ps) throws SQLException {
        Map<String, StoredRow> rows = new HashMap<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                rows.put(rs.getString(1), new StoredRow(rs.getBoolean(2), rs.getString(3)));
            }
        }
        return rows;
    }

    private void recordConflicts(Connection connection, String kind, List<String[]> conflicts) throws SQLException {
        if (conflicts.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO conflicts (run_id, kind, item_id, existing_hash, attempted_hash, at_us) VALUES (?, ?, ?, ?, ?, ?)")) {
            for (String[] c : conflicts) {
                ps.setString(1, c[0]);
                ps.setString(2, kind);
                ps.setString(3, c[1]);
                ps.setString(4, c[2]);
                ps.setString(5, c[3]);
                ps.setLong(6, nowMicros());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** Commit-time bookkeeping; asynchronous commit is fine because it is measurement data, not business data. */
    private void recordCommit(long batchId, String runId, int rows, long precommitUs, long postcommitUs) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement off = connection.prepareStatement("SET LOCAL synchronous_commit = off");
                 PreparedStatement ps = connection.prepareStatement(
                         "INSERT INTO commit_batches (batch_id, run_id, rows, precommit_us, postcommit_us) VALUES (?, ?, ?, ?, ?)")) {
                off.execute();
                ps.setLong(1, batchId);
                ps.setString(2, runId);
                ps.setInt(3, rows);
                ps.setLong(4, precommitUs);
                ps.setLong(5, postcommitUs);
                ps.executeUpdate();
            }
            connection.commit();
        }
    }

    public int insertEvents(List<BenchEvent> events) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(
                     "INSERT INTO bench_events (run_id, ts_us, source, type, detail) VALUES (?, ?, ?, ?, ?::jsonb)")) {
            for (BenchEvent event : events) {
                ps.setString(1, event.runId());
                ps.setLong(2, event.tsUs());
                ps.setString(3, event.source());
                ps.setString(4, event.type());
                ps.setString(5, event.detail() == null ? "{}" : event.detail());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        return events.size();
    }

    public Map<String, Object> summary(String runId) throws SQLException {
        Map<String, Object> summary = new LinkedHashMap<>();
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT kind, count(*), coalesce(sum(duplicate_attempts), 0) FROM action_receipts WHERE run_id = ? GROUP BY kind")) {
                ps.setString(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        summary.put(rs.getString(1).toLowerCase() + "Receipts", rs.getLong(2));
                        summary.put(rs.getString(1).toLowerCase() + "DuplicateAttempts", rs.getLong(3));
                    }
                }
            }
            try (PreparedStatement ps = connection.prepareStatement("""
                    SELECT count(*), coalesce(sum(duplicate_attempts), 0),
                           md5(coalesce(string_agg(checksum, '' ORDER BY correlation_id), ''))
                    FROM aggregates WHERE run_id = ?""")) {
                ps.setString(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    summary.put("aggregates", rs.getLong(1));
                    summary.put("aggregateDuplicateAttempts", rs.getLong(2));
                    summary.put("aggregateChecksumDigest", rs.getString(3));
                }
            }
            try (PreparedStatement ps = connection.prepareStatement("SELECT count(*) FROM conflicts WHERE run_id = ?")) {
                ps.setString(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    summary.put("conflicts", rs.getLong(1));
                }
            }
        }
        return summary;
    }

    private static <T> Array texts(Connection c, List<T> items, Function<T, String> f) throws SQLException {
        return c.createArrayOf("text", items.stream().map(f).toArray());
    }

    private static <T> Array ints(Connection c, List<T> items, Function<T, Integer> f) throws SQLException {
        return c.createArrayOf("int4", items.stream().map(f).toArray());
    }

    private static <T> Array longs(Connection c, List<T> items, Function<T, Long> f) throws SQLException {
        return c.createArrayOf("int8", items.stream().map(f).toArray());
    }

    static long nowMicros() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000L;
    }
}
