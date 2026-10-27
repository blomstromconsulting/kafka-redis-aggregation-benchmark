package io.aggbench.execution;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupMemberAssignment;
import org.apache.kafka.clients.admin.StreamsGroupMemberDescription;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.AppInfoParser;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Startup configuration report and "no silent fallback" guard.
 *
 * <p>After the application joined its groups, this asks the broker (Admin API) which group type each group
 * actually has (STREAMS, CLASSIC or CONSUMER), reads group-level configs (e.g. {@code streams.num.standby.replicas}
 * for A1) and the current member assignment. If anything differs from the expectation the failure callback runs
 * (the applications exit), so a benchmark can never silently run with the wrong protocol.
 * The report is logged, recorded as a benchmark event and served at {@code GET /status}.
 */
public final class ProtocolVerifier implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(ProtocolVerifier.class);

    public enum Status { PENDING, VERIFIED, FAILED }

    /** What the broker must report for one group. */
    public record Expectation(String groupId, GroupType type, Map<String, String> groupConfigs) {
    }

    private final Admin admin;
    private final List<Expectation> expectations;
    private final Map<String, Object> staticReport;
    private final BenchEvents events;
    private final Runnable onFailure;
    private final Duration timeout;
    private volatile Status status = Status.PENDING;
    private volatile Map<String, Object> report = Map.of();
    private volatile boolean running = true;
    private Thread thread;

    public ProtocolVerifier(String bootstrapServers, List<Expectation> expectations, Map<String, Object> staticReport,
                            BenchEvents events, Runnable onFailure, Duration timeout) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "protocol-verifier");
        this.admin = Admin.create(props);
        this.expectations = expectations;
        this.staticReport = staticReport;
        this.events = events;
        this.onFailure = onFailure;
        this.timeout = timeout;
    }

    public void start() {
        thread = Thread.ofPlatform().name("protocol-verifier").daemon().start(this::run);
    }

    public Status status() {
        return status;
    }

    public Map<String, Object> report() {
        return report;
    }

    private void run() {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (running && status == Status.PENDING) {
            try {
                Optional<String> problem = verifyOnce();
                if (problem.isEmpty()) {
                    status = Status.VERIFIED;
                    LOG.infof("Effective configuration verified: %s", io.aggbench.domain.Json.toString(report));
                    events.record("CONFIG_VERIFIED", report);
                } else if (System.nanoTime() > deadline || problem.get().startsWith("MISMATCH")) {
                    status = Status.FAILED;
                    LOG.errorf("Effective configuration does NOT match the requested variant: %s report=%s",
                            problem.get(), io.aggbench.domain.Json.toString(report));
                    events.record("CONFIG_VERIFICATION_FAILED", Map.of("problem", problem.get()));
                    onFailure.run();
                    return;
                }
            } catch (Exception e) {
                LOG.debugf("Group verification attempt failed: %s", e.toString());
            }
            sleep(2_000);
        }
        // Keep the membership part of the report fresh for /status.
        while (running) {
            sleep(15_000);
            try {
                verifyOnce();
            } catch (Exception e) {
                LOG.debugf("Group report refresh failed: %s", e.toString());
            }
        }
    }

    /** Returns empty when every expectation holds, otherwise a description of what is missing or wrong. */
    private Optional<String> verifyOnce() throws Exception {
        Map<String, Object> next = new LinkedHashMap<>(staticReport);
        next.put("kafkaClientVersion", AppInfoParser.getVersion());
        Map<String, GroupListing> listings = new TreeMap<>();
        for (GroupListing listing : admin.listGroups().all().get(10, TimeUnit.SECONDS)) {
            listings.put(listing.groupId(), listing);
        }
        Optional<String> problem = Optional.empty();
        List<Map<String, Object>> groups = new ArrayList<>();
        for (Expectation expectation : expectations) {
            Map<String, Object> group = new LinkedHashMap<>();
            group.put("groupId", expectation.groupId());
            group.put("expectedType", expectation.type().name());
            GroupListing listing = listings.get(expectation.groupId());
            GroupType actual = listing == null ? null : listing.type().orElse(GroupType.UNKNOWN);
            group.put("actualType", actual == null ? "NOT_FOUND" : actual.name());
            if (actual == null) {
                problem = problem.or(() -> Optional.of("PENDING: group " + expectation.groupId() + " not found yet"));
            } else if (actual != expectation.type()) {
                problem = problem.or(() -> Optional.of("MISMATCH: group " + expectation.groupId() + " is " + actual
                        + " but " + expectation.type() + " was requested"));
            } else {
                group.put("members", describeMembers(expectation.groupId(), actual));
            }
            if (!expectation.groupConfigs().isEmpty()) {
                Map<String, String> configs = groupConfigs(expectation.groupId());
                group.put("groupConfigs", configs);
                for (Map.Entry<String, String> expected : expectation.groupConfigs().entrySet()) {
                    String value = configs.get(expected.getKey());
                    if (!expected.getValue().equals(value)) {
                        String message = "MISMATCH: group config " + expected.getKey() + "=" + value + " but "
                                + expected.getValue() + " was requested for " + expectation.groupId();
                        problem = problem.or(() -> Optional.of(message));
                    }
                }
            }
            groups.add(group);
        }
        next.put("groups", groups);
        next.put("verificationStatus", problem.isEmpty() ? Status.VERIFIED.name() : problem.get());
        report = next;
        return problem;
    }

    private Map<String, String> groupConfigs(String groupId) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.GROUP, groupId);
        Config config = admin.describeConfigs(List.of(resource)).all().get(10, TimeUnit.SECONDS).get(resource);
        Map<String, String> values = new TreeMap<>();
        for (ConfigEntry entry : config.entries()) {
            values.put(entry.name(), entry.value());
        }
        return values;
    }

    private List<Map<String, Object>> describeMembers(String groupId, GroupType type) throws Exception {
        List<Map<String, Object>> members = new ArrayList<>();
        if (type == GroupType.STREAMS) {
            StreamsGroupDescription description = admin.describeStreamsGroups(List.of(groupId)).all()
                    .get(10, TimeUnit.SECONDS).get(groupId);
            for (StreamsGroupMemberDescription member : description.members()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("clientId", member.clientId());
                m.put("memberEpoch", member.memberEpoch());
                m.put("active", tasks(member.assignment().activeTasks()));
                m.put("standby", tasks(member.assignment().standbyTasks()));
                m.put("warmup", tasks(member.assignment().warmupTasks()));
                members.add(m);
            }
        } else {
            ConsumerGroupDescription description = admin.describeConsumerGroups(List.of(groupId)).all()
                    .get(10, TimeUnit.SECONDS).get(groupId);
            for (MemberDescription member : description.members()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("clientId", member.clientId());
                m.put("partitions", member.assignment().topicPartitions().stream()
                        .map(TopicPartition::partition).sorted().toList());
                members.add(m);
            }
        }
        return members;
    }

    private static List<String> tasks(List<StreamsGroupMemberAssignment.TaskIds> taskIds) {
        List<String> result = new ArrayList<>();
        for (StreamsGroupMemberAssignment.TaskIds ids : taskIds) {
            for (Integer partition : ids.partitions()) {
                result.add(ids.subtopologyId() + "_" + partition);
            }
        }
        result.sort(null);
        return result;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
        admin.close(Duration.ofSeconds(2));
    }
}
