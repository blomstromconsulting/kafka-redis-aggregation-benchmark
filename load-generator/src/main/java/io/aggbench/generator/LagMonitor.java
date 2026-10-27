package io.aggbench.generator;


import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsSpec;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Independent consumer-lag measurement, outside the implementations under test. Every sample reads the
 * group's committed offsets and the log end offsets through the Admin API, and the broker timestamp of the
 * first uncommitted record per lagging partition ("oldest unprocessed record age").
 *
 * <p>Committed offsets are the durability boundary in every variant (Streams commits with its EOS transaction;
 * the consumers commit after actions are durable), so lag here means "not yet durably processed".
 */
final class LagMonitor implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(LagMonitor.class);

    record Target(String groupId, String topic) {
    }

    private final Admin admin;
    private final KafkaConsumer<byte[], byte[]> peeker;
    private final List<Target> targets;
    private final Map<String, GroupType> groupTypes = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> lagGauges = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> ageGauges = new ConcurrentHashMap<>();
    private final MeterRegistry registry;

    LagMonitor(String bootstrapServers, List<Target> targets, MeterRegistry registry) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "lag-monitor");
        this.admin = Admin.create(props);
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "lag-monitor-peek");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        this.peeker = new KafkaConsumer<>(consumerProps);
        this.targets = targets;
        this.registry = registry;
    }

    List<RunRecorder.LagSample> sample() throws Exception {
        List<RunRecorder.LagSample> samples = new ArrayList<>();
        for (Target target : targets) {
            samples.add(sample(target));
        }
        return samples;
    }

    private RunRecorder.LagSample sample(Target target) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets(target.groupId());
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (var info : admin.describeTopics(List.of(target.topic())).allTopicNames().get(10, TimeUnit.SECONDS)
                .get(target.topic()).partitions()) {
            request.put(new TopicPartition(target.topic(), info.partition()), OffsetSpec.latest());
        }
        var ends = admin.listOffsets(request).all().get(10, TimeUnit.SECONDS);
        long total = 0;
        long maxLag = 0;
        long oldestAgeMs = 0;
        Map<Integer, Long> perPartition = new TreeMap<>();
        long now = System.currentTimeMillis();
        for (var entry : ends.entrySet()) {
            TopicPartition partition = entry.getKey();
            long end = entry.getValue().offset();
            OffsetAndMetadata c = committed.get(partition);
            long position = c == null ? 0 : c.offset();
            long lag = Math.max(0, end - position);
            perPartition.put(partition.partition(), lag);
            total += lag;
            maxLag = Math.max(maxLag, lag);
            if (lag > 0) {
                Long timestamp = timestampAt(partition, position);
                if (timestamp != null) {
                    oldestAgeMs = Math.max(oldestAgeMs, now - timestamp);
                }
            }
        }
        lagGauges.computeIfAbsent(target.groupId(), g -> registry.gauge("bench_group_lag_records",
                List.of(Tag.of("group", g)), new AtomicLong())).set(total);
        ageGauges.computeIfAbsent(target.groupId(), g -> registry.gauge("bench_oldest_unprocessed_age_ms",
                List.of(Tag.of("group", g)), new AtomicLong())).set(oldestAgeMs);
        return new RunRecorder.LagSample(LoadGenerator.nowMicros(), target.groupId(), target.topic(), total, maxLag, oldestAgeMs, perPartition);
    }

    private Map<TopicPartition, OffsetAndMetadata> committedOffsets(String groupId) throws Exception {
        GroupType type = groupTypes.get(groupId);
        if (type == null) {
            for (GroupListing listing : admin.listGroups().all().get(10, TimeUnit.SECONDS)) {
                if (listing.groupId().equals(groupId)) {
                    type = listing.type().orElse(GroupType.UNKNOWN);
                    groupTypes.put(groupId, type);
                }
            }
            if (type == null) {
                return Map.of();
            }
        }
        if (type == GroupType.STREAMS) {
            return admin.listStreamsGroupOffsets(Map.of(groupId, new ListStreamsGroupOffsetsSpec()))
                    .partitionsToOffsetAndMetadata(groupId).get(10, TimeUnit.SECONDS);
        }
        return admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
    }

    /** Broker timestamp of the record at {@code offset} (the first unprocessed one), or null. */
    private Long timestampAt(TopicPartition partition, long offset) {
        try {
            peeker.assign(List.of(partition));
            peeker.seek(partition, offset);
            for (ConsumerRecord<byte[], byte[]> record : peeker.poll(Duration.ofMillis(300))) {
                if (record.offset() >= offset) {
                    return record.timestamp();
                }
            }
        } catch (RuntimeException e) {
            LOG.debugf("Could not read %s@%d: %s", partition, offset, e.toString());
        }
        return null;
    }

    @Override
    public void close() {
        peeker.close(Duration.ofSeconds(2));
        admin.close(Duration.ofSeconds(2));
    }
}
