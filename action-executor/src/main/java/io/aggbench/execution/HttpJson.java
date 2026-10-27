package io.aggbench.execution;

import com.fasterxml.jackson.core.type.TypeReference;
import io.aggbench.domain.Json;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Minimal async JSON-over-HTTP helper on top of the JDK HTTP client. Non-200 responses fail the future. */
public final class HttpJson {

    /** A non-200 HTTP response. */
    public static final class HttpStatusException extends RuntimeException {
        private final int status;

        HttpStatusException(int status, String body) {
            super("HTTP " + status + ": " + (body.length() > 200 ? body.substring(0, 200) : body));
            this.status = status;
        }

        public int status() {
            return status;
        }
    }

    private final HttpClient client;

    public HttpJson(Duration connectTimeout) {
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(connectTimeout)
                .build();
    }

    public <T> CompletableFuture<T> post(String url, Object body, TypeReference<T> responseType, Duration timeout) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(Json.toBytes(body)))
                .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).thenApply(response -> {
            if (response.statusCode() != 200) {
                throw new HttpStatusException(response.statusCode(), new String(response.body()));
            }
            try {
                return Json.MAPPER.readValue(response.body(), responseType);
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        });
    }
}
