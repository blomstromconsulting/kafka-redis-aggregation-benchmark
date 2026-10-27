package io.aggbench.execution;

import io.aggbench.domain.PersistenceApi.ItemStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Natural batching for one persistence endpoint: items queue up while all request slots are busy and are
 * sent together when a slot frees up. There is no linger delay, so an idle system sends single-item batches.
 *
 * <p>Retry contract: a failed request (I/O error, timeout, non-200) re-queues every item of the batch after
 * an exponential backoff, without limit, until the item succeeds or the batcher is closed. The persistence
 * service is idempotent, so a retry after an unacknowledged success yields DUPLICATE, never a second outcome.
 */
final class Batcher<T> implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(Batcher.class);

    private record Pending<T>(T item, CompletableFuture<String> future, int attempt) {
    }

    private final String name;
    private final int maxBatchSize;
    private final Semaphore slots;
    private final Function<List<T>, CompletableFuture<List<ItemStatus>>> sender;
    private final ScheduledExecutorService scheduler;
    private final ExecutorSettings settings;
    private final LinkedBlockingQueue<Pending<T>> queue = new LinkedBlockingQueue<>();
    private final Counter retries;
    private final Counter failedRequests;
    private final Thread dispatcher;
    private volatile boolean running = true;

    Batcher(String name, int maxBatchSize, int maxConcurrentBatches,
            Function<List<T>, CompletableFuture<List<ItemStatus>>> sender,
            ScheduledExecutorService scheduler, ExecutorSettings settings, MeterRegistry registry) {
        this.name = name;
        this.maxBatchSize = maxBatchSize;
        this.slots = new Semaphore(maxConcurrentBatches);
        this.sender = sender;
        this.scheduler = scheduler;
        this.settings = settings;
        this.retries = Counter.builder("bench_action_retries_total").tag("endpoint", name).register(registry);
        this.failedRequests = Counter.builder("bench_persistence_request_failures_total").tag("endpoint", name).register(registry);
        registry.gauge("bench_persistence_queue_depth", List.of(io.micrometer.core.instrument.Tag.of("endpoint", name)), queue, LinkedBlockingQueue::size);
        this.dispatcher = Thread.ofPlatform().name("batcher-" + name).daemon().start(this::dispatchLoop);
    }

    /** Completes with the item status (CREATED, DUPLICATE or CONFLICT) once the item is durably handled. */
    CompletableFuture<String> submit(T item) {
        CompletableFuture<String> future = new CompletableFuture<>();
        if (!running) {
            future.completeExceptionally(new CancellationException(name + " closed"));
            return future;
        }
        queue.add(new Pending<>(item, future, 0));
        return future;
    }

    private void dispatchLoop() {
        while (running) {
            try {
                Pending<T> first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                slots.acquire();
                List<Pending<T>> batch = new ArrayList<>(Math.min(maxBatchSize, queue.size() + 1));
                batch.add(first);
                queue.drainTo(batch, maxBatchSize - 1);
                send(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void send(List<Pending<T>> batch) {
        List<T> items = new ArrayList<>(batch.size());
        for (Pending<T> pending : batch) {
            items.add(pending.item());
        }
        CompletableFuture<List<ItemStatus>> response;
        try {
            response = sender.apply(items);
        } catch (RuntimeException e) {
            response = CompletableFuture.failedFuture(e);
        }
        response.whenComplete((statuses, error) -> {
            slots.release();
            if (error != null || statuses == null || statuses.size() != batch.size()) {
                failedRequests.increment();
                LOG.debugf("%s request of %d items failed: %s", name, batch.size(), error);
                batch.forEach(this::retryLater);
                return;
            }
            for (int i = 0; i < batch.size(); i++) {
                batch.get(i).future().complete(statuses.get(i).status());
            }
        });
    }

    private void retryLater(Pending<T> pending) {
        if (!running) {
            pending.future().completeExceptionally(new CancellationException(name + " closed"));
            return;
        }
        retries.increment();
        int attempt = pending.attempt() + 1;
        if (attempt % 20 == 0) {
            LOG.warnf("%s item still failing after %d attempts", name, attempt);
        }
        scheduler.schedule(() -> queue.add(new Pending<>(pending.item(), pending.future(), attempt)),
                settings.backoff(attempt).toMillis(), TimeUnit.MILLISECONDS);
    }

    int queued() {
        return queue.size();
    }

    @Override
    public void close() {
        running = false;
        dispatcher.interrupt();
        Pending<T> pending;
        while ((pending = queue.poll()) != null) {
            pending.future().completeExceptionally(new CancellationException(name + " closed"));
        }
    }
}
