package io.aggbench.persistence;

import io.aggbench.domain.PersistenceApi.AggregateRecord;
import io.aggbench.domain.PersistenceApi.BenchEvent;
import io.aggbench.domain.PersistenceApi.ItemStatus;
import io.aggbench.domain.PersistenceApi.Receipt;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * HTTP API shared by all variants: durable action receipts, final aggregates, and benchmark timeline events.
 * Simulated latency and transient errors are applied identically for every variant ({@link FaultInjector}).
 */
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PersistenceResource {

    @Inject
    PersistenceRepository repository;

    @Inject
    FaultInjector faults;

    @Inject
    MeterRegistry registry;

    @POST
    @Path("api/receipts")
    public List<ItemStatus> receipts(List<Receipt> receipts) {
        return write("receipts", receipts.size(), () -> repository.insertReceipts(receipts));
    }

    @POST
    @Path("api/aggregates")
    public List<ItemStatus> aggregates(List<AggregateRecord> aggregates) {
        return write("aggregates", aggregates.size(), () -> repository.insertAggregates(aggregates));
    }

    private List<ItemStatus> write(String endpoint, int size, Callable<List<ItemStatus>> work) {
        if (size == 0) {
            return List.of();
        }
        faults.simulateLatency();
        if (faults.failBeforeWrite()) {
            registry.counter("bench_persistence_injected_errors_total", "endpoint", endpoint, "mode", "before").increment();
            throw new WebApplicationException("injected transient error (before write)", Response.Status.SERVICE_UNAVAILABLE);
        }
        Timer.Sample sample = Timer.start(registry);
        List<ItemStatus> statuses;
        try {
            statuses = work.call();
        } catch (SQLException e) {
            throw new WebApplicationException(e.getMessage(), Response.Status.SERVICE_UNAVAILABLE);
        } catch (Exception e) {
            throw new WebApplicationException(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
        }
        sample.stop(registry.timer("bench_persistence_batch_seconds", "endpoint", endpoint));
        registry.summary("bench_persistence_batch_size", "endpoint", endpoint).record(size);
        for (ItemStatus status : statuses) {
            registry.counter("bench_persistence_items_total", "endpoint", endpoint, "status", status.status()).increment();
        }
        if (faults.failAfterCommit()) {
            registry.counter("bench_persistence_injected_errors_total", "endpoint", endpoint, "mode", "after-commit").increment();
            throw new WebApplicationException("injected transient error (after commit)", Response.Status.SERVICE_UNAVAILABLE);
        }
        return statuses;
    }

    @POST
    @Path("api/events")
    public Map<String, Object> events(List<BenchEvent> events) throws SQLException {
        return Map.of("accepted", repository.insertEvents(events));
    }

    @GET
    @Path("api/runs/{runId}/summary")
    public Map<String, Object> summary(@PathParam("runId") String runId) throws SQLException {
        return repository.summary(runId);
    }

    @GET
    @Path("admin/faults")
    public FaultInjector.Faults getFaults() {
        return faults.current();
    }

    @POST
    @Path("admin/faults")
    public FaultInjector.Faults setFaults(FaultInjector.Faults next) {
        faults.update(next);
        return faults.current();
    }
}
