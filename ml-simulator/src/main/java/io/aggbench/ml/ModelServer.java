package io.aggbench.ml;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Simulated model server: a bounded number of concurrent "inference slots", a bounded FIFO queue in front of
 * them, and non-blocking simulated compute time (Vert.x timers, no thread is blocked while "computing").
 */
@ApplicationScoped
public class ModelServer {

    /** Thrown when the queue is full (mapped to HTTP 503). */
    public static final class Overloaded extends RuntimeException {
        Overloaded() {
            super("inference queue full");
        }
    }

    @Inject
    Vertx vertx;

    @Inject
    MeterRegistry registry;

    private volatile InferenceProfile profile;
    private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger queued = new AtomicInteger();
    private Timer serviceTime;
    private Timer queueTime;

    ModelServer(@ConfigProperty(name = "ml.profile", defaultValue = "fixed") String name,
                @ConfigProperty(name = "ml.latency-ms", defaultValue = "50") double latencyMs,
                @ConfigProperty(name = "ml.stddev-ms", defaultValue = "10") double stddevMs,
                @ConfigProperty(name = "ml.tail-probability", defaultValue = "0") double tailProbability,
                @ConfigProperty(name = "ml.tail-latency-ms", defaultValue = "0") double tailLatencyMs,
                @ConfigProperty(name = "ml.error-rate", defaultValue = "0") double errorRate,
                @ConfigProperty(name = "ml.timeout-rate", defaultValue = "0") double timeoutRate,
                @ConfigProperty(name = "ml.timeout-latency-ms", defaultValue = "5000") double timeoutLatencyMs,
                @ConfigProperty(name = "ml.max-concurrency", defaultValue = "256") int maxConcurrency,
                @ConfigProperty(name = "ml.max-queue", defaultValue = "10000") int maxQueue) {
        this.profile = new InferenceProfile(name, latencyMs, stddevMs, tailProbability, tailLatencyMs, errorRate,
                timeoutRate, timeoutLatencyMs, 1.0, maxConcurrency, maxQueue);
    }

    InferenceProfile profile() {
        return profile;
    }

    void update(InferenceProfile next) {
        next.sampleLatencyMs(); // validates the profile name
        this.profile = next;
    }

    <T> Uni<T> infer(Supplier<T> compute) {
        if (serviceTime == null) {
            serviceTime = Timer.builder("bench_ml_service_seconds").publishPercentileHistogram().register(registry);
            queueTime = Timer.builder("bench_ml_queue_seconds").publishPercentileHistogram().register(registry);
            registry.gauge("bench_ml_active", active);
            registry.gauge("bench_ml_queued", queued);
        }
        long enqueued = System.nanoTime();
        return Uni.createFrom().emitter(emitter -> {
            Runnable start = () -> {
                queueTime.record(System.nanoTime() - enqueued, TimeUnit.NANOSECONDS);
                InferenceProfile p = profile;
                long latency = p.sampleLatencyMs();
                boolean error = p.sampleError();
                long started = System.nanoTime();
                Runnable finish = () -> {
                    serviceTime.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
                    release();
                    if (error) {
                        emitter.fail(new IllegalStateException("injected inference error"));
                    } else {
                        emitter.complete(compute.get());
                    }
                };
                if (latency <= 0) {
                    finish.run();
                } else {
                    vertx.setTimer(latency, id -> finish.run());
                }
            };
            boolean runNow;
            synchronized (queue) {
                InferenceProfile p = profile;
                if (active.get() < p.maxConcurrency()) {
                    active.incrementAndGet();
                    runNow = true;
                } else if (queue.size() < p.maxQueue()) {
                    queue.add(start);
                    queued.incrementAndGet();
                    runNow = false;
                } else {
                    emitter.fail(new Overloaded());
                    return;
                }
            }
            if (runNow) {
                start.run();
            }
        });
    }

    private void release() {
        Runnable next;
        synchronized (queue) {
            next = queue.poll();
            if (next == null) {
                active.decrementAndGet();
            } else {
                queued.decrementAndGet();
            }
        }
        if (next != null) {
            next.run();
        }
    }
}
