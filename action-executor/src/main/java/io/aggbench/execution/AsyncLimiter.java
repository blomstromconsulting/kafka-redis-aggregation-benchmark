package io.aggbench.execution;

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Bounds the number of concurrently running async tasks; excess tasks wait in a FIFO queue. */
final class AsyncLimiter {

    private final int maxConcurrent;
    private final ArrayDeque<Runnable> waiting = new ArrayDeque<>();
    private int active;

    AsyncLimiter(int maxConcurrent) {
        this.maxConcurrent = maxConcurrent;
    }

    <T> CompletableFuture<T> submit(Supplier<CompletableFuture<T>> task) {
        CompletableFuture<T> result = new CompletableFuture<>();
        Runnable start = () -> {
            CompletableFuture<T> running;
            try {
                running = task.get();
            } catch (RuntimeException e) {
                running = CompletableFuture.failedFuture(e);
            }
            running.whenComplete((value, error) -> {
                release();
                if (error != null) {
                    result.completeExceptionally(error);
                } else {
                    result.complete(value);
                }
            });
        };
        boolean runNow;
        synchronized (this) {
            runNow = active < maxConcurrent;
            if (runNow) {
                active++;
            } else {
                waiting.add(start);
            }
        }
        if (runNow) {
            start.run();
        }
        return result;
    }

    private void release() {
        Runnable next;
        synchronized (this) {
            next = waiting.poll();
            if (next == null) {
                active--;
            }
        }
        if (next != null) {
            next.run();
        }
    }

    synchronized int active() {
        return active;
    }

    synchronized int queued() {
        return waiting.size();
    }
}
