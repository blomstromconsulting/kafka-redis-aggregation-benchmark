package io.aggbench.streams;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.eclipse.microprofile.health.Readiness;

import java.util.Map;

/**
 * HTTP surface: {@code /q/health/live}, {@code /q/health/ready} and {@code /status}.
 *
 * <p>Readiness means "this instance reached RUNNING once and its intent consumer joined"; it does NOT mean
 * every task is restored and processing. Task-level state (active, standby, restoring, standby lag) is exposed
 * separately on {@code /status} and as metrics.
 */
public final class StreamsEndpoints {

    private StreamsEndpoints() {
    }

    @Path("/status")
    public static class StatusResource {
        @Inject
        StreamsRuntime runtime;

        @GET
        @Produces(MediaType.APPLICATION_JSON)
        public Map<String, Object> status() {
            return runtime.status();
        }
    }

    @Liveness
    @ApplicationScoped
    public static class Live implements HealthCheck {
        @Inject
        StreamsRuntime runtime;

        @Override
        public HealthCheckResponse call() {
            return HealthCheckResponse.named("streams-alive").status(runtime.alive())
                    .withData("state", runtime.state().name()).build();
        }
    }

    @Readiness
    @ApplicationScoped
    public static class Ready implements HealthCheck {
        @Inject
        StreamsRuntime runtime;

        @Override
        public HealthCheckResponse call() {
            return HealthCheckResponse.named("streams-ready").status(runtime.ready())
                    .withData("state", runtime.state().name()).build();
        }
    }
}
