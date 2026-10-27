package io.aggbench.execution;

import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.aggbench.domain.ActionIntent;
import io.aggbench.domain.ActionKind;
import io.aggbench.domain.Hashing;
import io.aggbench.domain.InferenceModel;
import io.aggbench.domain.Json;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises retries and idempotency against an in-process fake persistence + inference server. */
class ActionExecutorTest {

    private HttpServer server;
    private final Set<String> stored = ConcurrentHashMap.newKeySet();
    private final Map<String, Double> scores = new ConcurrentHashMap<>();
    private final AtomicInteger receiptCalls = new AtomicInteger();
    private final AtomicInteger failFirstCalls = new AtomicInteger();
    private final AtomicInteger failAfterCommitCalls = new AtomicInteger();
    private final AtomicInteger inferenceFailures = new AtomicInteger();
    private ActionExecutor executor;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/receipts", exchange -> persist(exchange, "actionId"));
        server.createContext("/api/aggregates", exchange -> persist(exchange, "correlationId"));
        server.createContext("/v2/models/" + InferenceModel.MODEL_NAME + "/infer", this::infer);
        server.start();
        String base = "http://127.0.0.1:" + server.getAddress().getPort();
        executor = new ActionExecutor(new ExecutorSettings(base, base, "test", "pod-1", 50, 2, 4,
                Duration.ofSeconds(2), Duration.ofSeconds(2), Duration.ofMillis(5), Duration.ofMillis(50)),
                new SimpleMeterRegistry());
    }

    @AfterEach
    void stop() {
        executor.close();
        server.stop(0);
    }

    @Test
    void retriesTransientFailuresAndCountsDuplicatesOnce() throws Exception {
        failFirstCalls.set(2);        // fail before writing
        failAfterCommitCalls.set(1);  // write, then report failure: the retry must observe DUPLICATE
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(executor.execute(intent(ActionKind.INDIVIDUAL, "corr-" + i, false)));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
        assertEquals(100, stored.size());
        assertTrue(receiptCalls.get() > 3, "retries must have happened");
        assertEquals(0, executor.inFlight());
    }

    @Test
    void finalActionRunsInferenceWithRetryThenPersists() throws Exception {
        inferenceFailures.set(3);
        ActionIntent intent = intent(ActionKind.FINAL, "corr-final", true);
        executor.execute(intent).get(10, TimeUnit.SECONDS);
        assertTrue(stored.contains("corr-final"));
        double expected = InferenceModel.score(InferenceModel.features(intent.contentHash()));
        assertEquals(expected, scores.get("corr-final"), 1e-9);
    }

    private static ActionIntent intent(ActionKind kind, String corr, boolean inference) {
        String checksum = Hashing.sha256(corr);
        String actionId = switch (kind) {
            case INDIVIDUAL -> ActionKind.individualId(corr + ":1:v1");
            case STAGE2 -> ActionKind.stage2Id(corr);
            case FINAL -> ActionKind.finalId(corr);
        };
        return new ActionIntent(actionId, kind, "run", corr, kind == ActionKind.INDIVIDUAL ? 1 : 0,
                List.of(corr + ":1:v1"), checksum, "a", "b", kind == ActionKind.FINAL ? "c" : null, inference,
                0, System.currentTimeMillis(), System.currentTimeMillis(), Micros.now());
    }

    private void persist(HttpExchange exchange, String idField) throws IOException {
        receiptCalls.incrementAndGet();
        List<Map<String, Object>> items = Json.MAPPER.readValue(exchange.getRequestBody(), new TypeReference<>() {
        });
        if (failFirstCalls.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            respond(exchange, 503, "{}");
            return;
        }
        List<Map<String, String>> statuses = new ArrayList<>();
        for (Map<String, Object> item : items) {
            String id = (String) item.get(idField);
            boolean created = stored.add(id);
            if (item.get("mlScore") instanceof Number score) {
                scores.put(id, score.doubleValue());
            }
            statuses.add(Map.of("id", id, "status", created ? "CREATED" : "DUPLICATE"));
        }
        if (failAfterCommitCalls.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            respond(exchange, 500, "{}");
            return;
        }
        respond(exchange, 200, Json.toString(statuses));
    }

    private void infer(HttpExchange exchange) throws IOException {
        Map<String, Object> request = Json.MAPPER.readValue(exchange.getRequestBody(), new TypeReference<>() {
        });
        if (inferenceFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            respond(exchange, 503, "{}");
            return;
        }
        @SuppressWarnings("unchecked")
        List<Number> data = (List<Number>) ((List<Map<String, Object>>) request.get("inputs")).getFirst().get("data");
        double[] features = data.stream().mapToDouble(Number::doubleValue).toArray();
        double score = InferenceModel.score(features);
        respond(exchange, 200, Json.toString(Map.of("model_name", InferenceModel.MODEL_NAME,
                "outputs", List.of(Map.of("name", "score", "shape", List.of(1, 1), "datatype", "FP64", "data", List.of(score))))));
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes();
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
