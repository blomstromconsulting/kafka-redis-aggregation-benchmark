package io.aggbench.redis;

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

/** HTTP surface: {@code /q/health/live}, {@code /q/health/ready} (consumer joined and polling) and {@code /status}. */
public final class RedisEndpoints {

    private RedisEndpoints() {
    }

    @Path("/status")
    public static class StatusResource {
        @Inject
        RedisRuntime runtime;

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
        RedisRuntime runtime;

        @Override
        public HealthCheckResponse call() {
            return HealthCheckResponse.named("consumer-alive").status(runtime.alive()).build();
        }
    }

    @Readiness
    @ApplicationScoped
    public static class Ready implements HealthCheck {
        @Inject
        RedisRuntime runtime;

        @Override
        public HealthCheckResponse call() {
            return HealthCheckResponse.named("consumer-ready").status(runtime.ready()).build();
        }
    }
}
