package io.aggbench.redis;

import io.aggbench.domain.AggregationState;
import io.aggbench.domain.FragmentEvent;
import io.aggbench.domain.FragmentType;
import io.aggbench.domain.IngressPosition;
import io.aggbench.domain.MergeOutcome;
import io.aggbench.domain.StoredFragment;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The Redis adapter of variant B: the only class that talks to Redis.
 *
 * <p>{@link #merge} calls the atomic {@code aggbench_merge} Redis Function ({@code redis/aggregation.lua}).
 * Commands from the consumer thread are pipelined on one connection, so merges for the same key reach Redis in
 * partition order. A merge is idempotent: retrying it, or replaying the same Kafka record, yields REDELIVERED.
 */
public final class RedisAggregationStore implements AutoCloseable {

    /** Reply of one merge. {@code state} is only populated when stages are due (it carries their payloads). */
    public record MergeReply(MergeOutcome outcome, int stagesDue, AggregationState state) {
    }

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisAsyncCommands<String, String> commands;
    private final String keyPrefix;
    private final String openKey;
    private final long incompleteTtlMs;
    private final long completedRetentionMs;

    public RedisAggregationStore(String redisUri, String runId, Duration commandTimeout, Duration incompleteTtl,
                                 Duration completedRetention) {
        RedisURI uri = RedisURI.create(redisUri);
        uri.setTimeout(commandTimeout);
        this.client = RedisClient.create(uri);
        this.connection = client.connect();
        this.commands = connection.async();
        this.keyPrefix = "ab:" + runId + ":";
        this.openKey = "ab:" + runId + ":open";
        this.incompleteTtlMs = incompleteTtl.toMillis();
        this.completedRetentionMs = completedRetention.toMillis();
    }

    /** Loads (or replaces) the function library. Safe to call from every pod at startup. */
    public void loadFunctions() {
        connection.sync().functionLoad(libraryCode(), true);
    }

    public static String libraryCode() {
        try (InputStream in = RedisAggregationStore.class.getResourceAsStream("/redis/aggregation.lua")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public CompletableFuture<MergeReply> merge(FragmentEvent event, IngressPosition position, String payloadHash, long nowMs) {
        String[] keys = {keyPrefix + event.correlationId(), openKey};
        return commands.<List<Object>>fcall("aggbench_merge", ScriptOutputType.MULTI, keys,
                        event.runId(), event.correlationId(), String.valueOf(event.eventType()), event.eventId(),
                        String.valueOf(event.version()), event.payload(), payloadHash,
                        event.requiresInference() ? "1" : "0", String.valueOf(position.appendTimeMs()),
                        position.recordRef(), String.valueOf(nowMs), String.valueOf(incompleteTtlMs),
                        String.valueOf(completedRetentionMs))
                .toCompletableFuture()
                .thenApply(RedisAggregationStore::parse);
    }

    @SuppressWarnings("unchecked")
    static MergeReply parse(List<Object> reply) {
        MergeOutcome outcome = MergeOutcome.valueOf((String) reply.get(0));
        int stages = ((Number) reply.get(1)).intValue();
        AggregationState state = null;
        if (reply.size() > 2) {
            List<Object> flat = (List<Object>) reply.get(2);
            Map<String, String> fields = new HashMap<>();
            for (int i = 0; i + 1 < flat.size(); i += 2) {
                fields.put((String) flat.get(i), (String) flat.get(i + 1));
            }
            state = toState(fields);
        }
        return new MergeReply(outcome, stages, state);
    }

    private static AggregationState toState(Map<String, String> h) {
        return new AggregationState(h.get("run"), h.get("corr"),
                fragment(h, FragmentType.A), fragment(h, FragmentType.B), fragment(h, FragmentType.C), List.of(),
                "1".equals(h.get("s2")), "1".equals(h.get("fin")),
                Long.parseLong(h.getOrDefault("created", "0")), Long.parseLong(h.getOrDefault("completed", "0")),
                Integer.parseInt(h.getOrDefault("dup", "0")), Integer.parseInt(h.getOrDefault("stale", "0")),
                Integer.parseInt(h.getOrDefault("late", "0")));
    }

    private static StoredFragment fragment(Map<String, String> h, FragmentType type) {
        String t = type.name().toLowerCase();
        String id = h.get(t + ":id");
        if (id == null) {
            return null;
        }
        return new StoredFragment(id, Integer.parseInt(h.get(t + ":v")), h.get(t + ":p"), h.get(t + ":h"),
                "1".equals(h.get(t + ":inf")), Long.parseLong(h.get(t + ":ts")), h.get(t + ":ref"),
                Integer.parseInt(h.get(t + ":trig")));
    }

    /** Removes expired entries from the open set; returns how many this call removed. */
    public long sweepExpiredIncomplete(long nowMs, int limit) {
        Long removed = connection.sync().fcall("aggbench_sweep_expired", ScriptOutputType.INTEGER,
                new String[]{openKey}, String.valueOf(nowMs - incompleteTtlMs), String.valueOf(limit));
        return removed == null ? 0 : removed;
    }

    public long openAggregates() {
        return connection.sync().zcard(openKey);
    }

    /** {@code run_id} of the server we are connected to; changes on restart and on failover. */
    public String serverRunId() {
        return infoField(connection.sync().info("server"), "run_id");
    }

    public long usedMemoryBytes() {
        String value = infoField(connection.sync().info("memory"), "used_memory");
        return value == null ? -1 : Long.parseLong(value);
    }

    static String infoField(String info, String field) {
        for (String line : info.split("\r?\n")) {
            if (line.startsWith(field + ":")) {
                return line.substring(field.length() + 1).trim();
            }
        }
        return null;
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown(Duration.ZERO, Duration.ofSeconds(2));
    }
}
