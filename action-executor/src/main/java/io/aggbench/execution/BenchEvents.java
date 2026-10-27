package io.aggbench.execution;

import com.fasterxml.jackson.core.type.TypeReference;
import io.aggbench.domain.Json;
import io.aggbench.domain.PersistenceApi.BenchEvent;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Records benchmark timeline events (assignment changes, restoration start/end, processing resumption,
 * Redis epoch changes). Every event is logged as a structured line immediately and shipped best-effort to
 * the persistence service every second, where the report reads it. Losing an event never affects correctness.
 */
public final class BenchEvents implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger("bench.events");

    private final String runId;
    private final String source;
    private final String url;
    private final HttpJson http = new HttpJson(Duration.ofSeconds(2));
    private final ConcurrentLinkedQueue<BenchEvent> queue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService flusher;

    public BenchEvents(String runId, String source, String persistenceUrl) {
        this.runId = runId;
        this.source = source;
        this.url = persistenceUrl + "/api/events";
        this.flusher = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("bench-events").daemon().factory());
        this.flusher.scheduleWithFixedDelay(this::flush, 1, 1, TimeUnit.SECONDS);
    }

    public void record(String type, Map<String, ?> detail) {
        String json = Json.toString(detail);
        LOG.infof("bench-event type=%s source=%s detail=%s", type, source, json);
        queue.add(new BenchEvent(runId, Micros.now(), source, type, json));
    }

    private void flush() {
        List<BenchEvent> batch = new ArrayList<>();
        BenchEvent event;
        while ((event = queue.poll()) != null && batch.size() < 500) {
            batch.add(event);
        }
        if (batch.isEmpty()) {
            return;
        }
        try {
            http.post(url, batch, new TypeReference<Map<String, Object>>() {
            }, Duration.ofSeconds(5)).get(6, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.debugf("Could not ship %d bench events: %s", batch.size(), e.toString());
            if (queue.size() < 10_000) {
                queue.addAll(batch);
            }
        }
    }

    @Override
    public void close() {
        flush();
        flusher.shutdownNow();
    }
}
