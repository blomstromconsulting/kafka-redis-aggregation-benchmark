package io.aggbench.execution;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.ToLongFunction;

/**
 * Tracks records whose work is still running so that offsets are committed only up to the first
 * unfinished record of each partition ("commit after durable completion").
 *
 * <p>{@link #register} is called by the consumer thread in offset order before work starts;
 * {@link #complete} may be called from any thread. Commit positions are read by the consumer thread.
 */
public final class OffsetTracker {

    private static final class PartitionProgress {
        final ConcurrentSkipListSet<Long> pending = new ConcurrentSkipListSet<>();
        volatile long nextOffset = -1;   // highest registered offset + 1
        long committedPosition = -1;     // last position handed out for commit (consumer thread only)

        long position() {
            Long first = pending.isEmpty() ? null : pending.first();
            return first != null ? first : nextOffset;
        }
    }

    private final Map<TopicPartition, PartitionProgress> partitions = new ConcurrentHashMap<>();

    public void register(TopicPartition partition, long offset) {
        PartitionProgress progress = partitions.computeIfAbsent(partition, p -> new PartitionProgress());
        progress.pending.add(offset);
        progress.nextOffset = offset + 1;
    }

    public void complete(TopicPartition partition, long offset) {
        PartitionProgress progress = partitions.get(partition);
        if (progress != null) {
            progress.pending.remove(offset);
        }
    }

    /** Positions that advanced since the last call, ready to be committed. */
    public Map<TopicPartition, OffsetAndMetadata> committable() {
        return committable(partition -> -1);
    }

    /**
     * Positions that advanced since the last call. For a partition with no unfinished work the consumer's fetch
     * position is used when it is ahead of the last record: on transactional topics the commit marker after the
     * last record occupies an offset, and committing only "last record + 1" would leave a permanent lag of one.
     * Must be called on the consumer thread (the only thread that registers records).
     */
    public Map<TopicPartition, OffsetAndMetadata> committable(ToLongFunction<TopicPartition> idlePosition) {
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        partitions.forEach((partition, progress) -> {
            long position = position(partition, progress, idlePosition);
            if (position > progress.committedPosition) {
                result.put(partition, new OffsetAndMetadata(position));
                progress.committedPosition = position;
            }
        });
        return result;
    }

    /** Positions for the given partitions regardless of whether they advanced. */
    public Map<TopicPartition, OffsetAndMetadata> positions(Collection<TopicPartition> subset,
                                                            ToLongFunction<TopicPartition> idlePosition) {
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        for (TopicPartition partition : subset) {
            PartitionProgress progress = partitions.get(partition);
            if (progress != null) {
                long position = position(partition, progress, idlePosition);
                if (position >= 0) {
                    result.put(partition, new OffsetAndMetadata(position));
                }
            }
        }
        return result;
    }

    private static long position(TopicPartition partition, PartitionProgress progress, ToLongFunction<TopicPartition> idlePosition) {
        long tracked = progress.position();
        if (progress.pending.isEmpty() && progress.nextOffset >= 0) {
            return Math.max(tracked, idlePosition.applyAsLong(partition));
        }
        return tracked;
    }

    public int inFlight() {
        int total = 0;
        for (PartitionProgress progress : partitions.values()) {
            total += progress.pending.size();
        }
        return total;
    }

    public int inFlight(Collection<TopicPartition> subset) {
        int total = 0;
        for (TopicPartition partition : subset) {
            PartitionProgress progress = partitions.get(partition);
            if (progress != null) {
                total += progress.pending.size();
            }
        }
        return total;
    }

    /** Waits until no work is pending for the given partitions. Returns false on timeout. */
    public boolean awaitIdle(Collection<TopicPartition> subset, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (inFlight(subset) > 0) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    /** Forgets partitions (revoked or lost). Late completions are ignored. */
    public void remove(Collection<TopicPartition> subset) {
        subset.forEach(partitions::remove);
    }

    /**
     * Restarts tracking before a replay from an earlier offset. The last committed position is kept so that
     * replayed records never move the committed offset backwards.
     */
    public void reset(Collection<TopicPartition> subset) {
        for (TopicPartition partition : subset) {
            PartitionProgress old = partitions.get(partition);
            PartitionProgress fresh = new PartitionProgress();
            if (old != null) {
                fresh.committedPosition = old.committedPosition;
            }
            partitions.put(partition, fresh);
        }
    }
}
