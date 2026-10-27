package io.aggbench.execution;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

/**
 * A plain {@link KafkaConsumer} loop with asynchronous record handling and commit-after-completion.
 * Used for the Redis variant's ingress consumer and for the Kafka Streams variant's action-intent consumer.
 *
 * <p>Acknowledgement logic (the durability boundary):
 * <ol>
 *   <li>Each polled record is registered in the {@link OffsetTracker} before its work starts.</li>
 *   <li>The {@link RecordHandler} returns one future per record that completes when all of the record's
 *       effects are durable (state merged and every triggered action confirmed by the persistence service).</li>
 *   <li>Every {@code commitInterval} the loop commits, per partition, the offset of the first unfinished
 *       record. A crash therefore only causes redelivery of unfinished or uncommitted records.</li>
 *   <li>On revocation the loop waits (bounded) for the revoked partitions' work, then commits synchronously;
 *       lost partitions are dropped without commit.</li>
 * </ol>
 * Bounded in-flight work: when the supplied in-flight gauge (business actions not yet durable, the same unit for
 * every variant) reaches {@code maxInFlight}, the assignment is paused until it drains below 80 %.
 */
public final class ManagedConsumerLoop implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(ManagedConsumerLoop.class);

    /**
     * Handles the records of one poll on the consumer thread; returns one completion per record, same order.
     * {@code completedBefore} is true for records that are being re-read by a replay and whose work had already
     * completed durably before the replay started (their offsets were reached before the seek-back).
     */
    public interface RecordHandler {
        List<CompletableFuture<?>> handle(List<ConsumerRecord<String, byte[]>> records,
                                          Predicate<ConsumerRecord<String, byte[]>> completedBefore);
    }

    public record LoopSettings(int maxInFlight, Duration commitInterval, Duration revokeDrainTimeout,
                               Duration shutdownDrainTimeout) {
    }

    private final String name;
    private final Properties properties;
    private final Collection<String> topics;
    private final RecordHandler handler;
    private final LoopSettings settings;
    private final BenchEvents events;
    private final IntSupplier inFlightWork;
    private final OffsetTracker tracker = new OffsetTracker();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final Map<TopicPartition, Long> assignedAtMs = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Long> replayEndOffsets = new ConcurrentHashMap<>();
    private final AtomicLong replayFromTimestampMs = new AtomicLong(-1);
    private final Counter polledRecords;
    private final Counter rebalanceAssigned;
    private final Counter rebalanceRevoked;
    private final Counter rebalanceLost;
    private final Counter handlerFailures;
    private final MeterRegistry registry;

    private volatile boolean running = true;
    private volatile boolean polledOnce;
    private volatile boolean failed;
    private volatile int assignedPartitions;
    private volatile boolean paused;
    private KafkaConsumer<String, byte[]> consumer;
    private Thread thread;

    public ManagedConsumerLoop(String name, Properties properties, Collection<String> topics, RecordHandler handler,
                               LoopSettings settings, IntSupplier inFlightWork, BenchEvents events, MeterRegistry registry) {
        this.name = name;
        this.properties = properties;
        this.topics = topics;
        this.handler = handler;
        this.settings = settings;
        this.events = events;
        this.inFlightWork = inFlightWork;
        this.registry = registry;
        this.polledRecords = Counter.builder("bench_consumer_records_total").tag("loop", name).register(registry);
        this.rebalanceAssigned = Counter.builder("bench_partition_assignments_total").tag("loop", name).tag("change", "assigned").register(registry);
        this.rebalanceRevoked = Counter.builder("bench_partition_assignments_total").tag("loop", name).tag("change", "revoked").register(registry);
        this.rebalanceLost = Counter.builder("bench_partition_assignments_total").tag("loop", name).tag("change", "lost").register(registry);
        this.handlerFailures = Counter.builder("bench_handler_failures_total").tag("loop", name).register(registry);
        registry.gauge("bench_consumer_inflight_records", List.of(io.micrometer.core.instrument.Tag.of("loop", name)), tracker, OffsetTracker::inFlight);
        registry.gauge("bench_consumer_assigned_partitions", List.of(io.micrometer.core.instrument.Tag.of("loop", name)), this, l -> l.assignedPartitions);
        registry.gauge("bench_consumer_paused", List.of(io.micrometer.core.instrument.Tag.of("loop", name)), this, l -> l.paused ? 1 : 0);
    }

    public void start() {
        thread = Thread.ofPlatform().name("consumer-" + name).start(this::run);
    }

    /** True once the consumer joined its group and completed a poll. */
    public boolean started() {
        return polledOnce && !failed;
    }

    public boolean healthy() {
        return !failed;
    }

    public int assignedPartitions() {
        return assignedPartitions;
    }

    public int inFlight() {
        return tracker.inFlight();
    }

    /**
     * Asks the consumer thread to re-read all assigned partitions from the given broker timestamp.
     * Used by the Redis variant after detecting a Redis restart/failover that may have lost state.
     */
    public void requestReplayFrom(long timestampMs) {
        replayFromTimestampMs.accumulateAndGet(timestampMs, (current, requested) -> current < 0 ? requested : Math.min(current, requested));
    }

    private void run() {
        try (KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(properties)) {
            consumer = kafkaConsumer;
            new KafkaClientMetrics(kafkaConsumer).bindTo(registry);
            consumer.subscribe(topics, new Listener());
            long lastCommitNanos = System.nanoTime();
            while (running) {
                applyReplayRequest();
                applyFlowControl();
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                polledOnce = true;
                if (!records.isEmpty()) {
                    dispatch(records);
                }
                if (System.nanoTime() - lastCommitNanos >= settings.commitInterval().toNanos()) {
                    Map<TopicPartition, OffsetAndMetadata> commit = tracker.committable(this::fetchPosition);
                    if (!commit.isEmpty()) {
                        consumer.commitAsync(commit, (offsets, error) -> {
                            if (error != null) {
                                LOG.debugf("%s async commit failed: %s", name, error.toString());
                            }
                        });
                    }
                    lastCommitNanos = System.nanoTime();
                }
            }
            shutdown();
        } catch (WakeupException e) {
            if (running) {
                failed = true;
                LOG.error(name + " consumer woken up unexpectedly", e);
            }
        } catch (RuntimeException e) {
            failed = true;
            LOG.error(name + " consumer loop failed", e);
        } finally {
            stopped.countDown();
        }
    }

    private void dispatch(ConsumerRecords<String, byte[]> records) {
        List<ConsumerRecord<String, byte[]>> batch = new ArrayList<>(records.count());
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(partition);
            Long assignedAt = assignedAtMs.remove(partition);
            if (assignedAt != null) {
                ConsumerRecord<String, byte[]> first = partitionRecords.getFirst();
                events.record("PARTITION_PROCESSING_STARTED", Map.of(
                        "loop", name, "partition", partition.partition(), "offset", first.offset(),
                        "msSinceAssignment", System.currentTimeMillis() - assignedAt,
                        "firstRecordAgeMs", System.currentTimeMillis() - first.timestamp()));
            }
            for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                tracker.register(partition, record.offset());
                batch.add(record);
            }
        }
        polledRecords.increment(batch.size());
        List<CompletableFuture<?>> completions = handler.handle(batch, record -> {
            Long replayEnd = replayEndOffsets.get(new TopicPartition(record.topic(), record.partition()));
            return replayEnd != null && record.offset() < replayEnd;
        });
        for (int i = 0; i < batch.size(); i++) {
            ConsumerRecord<String, byte[]> record = batch.get(i);
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            completions.get(i).whenComplete((ignored, error) -> {
                if (error == null) {
                    tracker.complete(partition, record.offset());
                } else if (running) {
                    // Leave the offset uncommitted: the record will be redelivered after a restart.
                    handlerFailures.increment();
                    failed = true;
                    LOG.errorf(error, "%s: work for %s@%d failed permanently; marking consumer unhealthy", name, partition, record.offset());
                }
            });
        }
    }

    /** The consumer's fetch position for an assigned partition, or -1 (consumer thread only). */
    private long fetchPosition(TopicPartition partition) {
        try {
            return consumer.assignment().contains(partition) ? consumer.position(partition, Duration.ZERO) : -1;
        } catch (RuntimeException e) {
            return -1;
        }
    }

    private void applyFlowControl() {
        int inFlight = inFlightWork.getAsInt();
        if (!paused && inFlight >= settings.maxInFlight()) {
            consumer.pause(consumer.assignment());
            paused = true;
        } else if (paused && inFlight < settings.maxInFlight() * 0.8) {
            consumer.resume(consumer.paused());
            paused = false;
        }
    }

    private void applyReplayRequest() {
        long timestamp = replayFromTimestampMs.getAndSet(-1);
        if (timestamp < 0) {
            return;
        }
        Set<TopicPartition> assignment = new HashSet<>(consumer.assignment());
        if (assignment.isEmpty()) {
            return;
        }
        boolean idle = tracker.awaitIdle(assignment, settings.revokeDrainTimeout());
        Map<TopicPartition, Long> query = new HashMap<>();
        assignment.forEach(p -> query.put(p, timestamp));
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(query);
        Map<String, Object> detail = new HashMap<>();
        offsets.forEach((partition, offsetAndTimestamp) -> {
            long position = consumer.position(partition);
            if (offsetAndTimestamp != null && offsetAndTimestamp.offset() < position) {
                detail.put(String.valueOf(partition.partition()), position - offsetAndTimestamp.offset());
                replayEndOffsets.merge(partition, position, Math::max);
                consumer.seek(partition, offsetAndTimestamp.offset());
            }
        });
        tracker.reset(assignment);
        events.record("REPLAY_STARTED", Map.of("loop", name, "fromTimestampMs", timestamp, "drained", idle,
                "recordsToReplayByPartition", detail));
    }

    private void shutdown() {
        Set<TopicPartition> assignment = consumer.assignment();
        boolean idle = tracker.awaitIdle(assignment, settings.shutdownDrainTimeout());
        Map<TopicPartition, OffsetAndMetadata> positions = tracker.positions(assignment, this::fetchPosition);
        if (!positions.isEmpty()) {
            try {
                consumer.commitSync(positions, Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                LOG.warnf("%s final commit failed: %s", name, e.toString());
            }
        }
        events.record("CONSUMER_SHUTDOWN", Map.of("loop", name, "drained", idle, "partitions", assignment.size()));
    }

    private final class Listener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty()) {
                return;
            }
            long now = System.currentTimeMillis();
            partitions.forEach(p -> assignedAtMs.put(p, now));
            rebalanceAssigned.increment(partitions.size());
            assignedPartitions = consumer.assignment().size();
            if (paused) {
                consumer.pause(partitions);
            }
            events.record("PARTITIONS_ASSIGNED", Map.of("loop", name, "partitions", ids(partitions),
                    "assignmentSize", assignedPartitions));
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty()) {
                return;
            }
            long start = System.currentTimeMillis();
            boolean idle = tracker.awaitIdle(partitions, settings.revokeDrainTimeout());
            Map<TopicPartition, OffsetAndMetadata> positions = tracker.positions(partitions, ManagedConsumerLoop.this::fetchPosition);
            if (!positions.isEmpty()) {
                try {
                    consumer.commitSync(positions, Duration.ofSeconds(5));
                } catch (RuntimeException e) {
                    LOG.warnf("%s commit on revoke failed: %s", name, e.toString());
                }
            }
            tracker.remove(partitions);
            partitions.forEach(assignedAtMs::remove);
            partitions.forEach(replayEndOffsets::remove);
            rebalanceRevoked.increment(partitions.size());
            assignedPartitions = Math.max(0, consumer.assignment().size() - partitions.size());
            events.record("PARTITIONS_REVOKED", Map.of("loop", name, "partitions", ids(partitions),
                    "drained", idle, "revokeMs", System.currentTimeMillis() - start));
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            tracker.remove(partitions);
            partitions.forEach(assignedAtMs::remove);
            partitions.forEach(replayEndOffsets::remove);
            rebalanceLost.increment(partitions.size());
            events.record("PARTITIONS_LOST", Map.of("loop", name, "partitions", ids(partitions)));
        }
    }

    private static List<Integer> ids(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::partition).sorted().toList();
    }

    @Override
    public void close() {
        running = false;
        try {
            if (!stopped.await(settings.shutdownDrainTimeout().toMillis() + 10_000, TimeUnit.MILLISECONDS)) {
                LOG.warnf("%s did not stop in time", name);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
