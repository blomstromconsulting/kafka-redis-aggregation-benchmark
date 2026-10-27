package io.aggbench.generator;

import io.aggbench.domain.FragmentEvent;
import io.aggbench.domain.FragmentType;
import io.aggbench.domain.Hashing;
import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SplittableRandom;

/**
 * The deterministic logical workload. Everything about aggregate {@code i} (correlation id, partition,
 * fragment order and timing, duplicates, stale versions, missing fragments, payloads) is a pure function of
 * {@code (seed, settings, i)}. A1, A2 and B therefore receive exactly the same logical input when they are run
 * with the same seed and settings; only the run id differs.
 *
 * <p>Arrival schedule: aggregates start at a constant rate, after a linear ramp from 10 % to 100 % during the
 * first {@code rampSeconds} of warm-up. The aggregate rate is {@code eventRate / meanEventsPerAggregate}, so the
 * offered fragment rate (including duplicates and stale events) equals {@code eventRate}.
 */
public final class WorkloadPlan {

    private static final double RAMP_START_FRACTION = 0.1;
    private static final char[] ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    public record Settings(long seed, double eventRate, int warmupSeconds, int rampSeconds, int durationSeconds,
                           int payloadSizeA, int payloadSizeB, int payloadSizeC,
                           long correlationWindowMs, String windowDistribution,
                           double outOfOrderPercent, double duplicatePercent, long duplicateDelayMaxMs,
                           double missingPercent, double stalePercent, double inferencePercent,
                           int partitions, double hotPartitionFraction, double hotTrafficFraction) {
    }

    /** One fragment record to send, at {@code offsetUs} after the aggregate's start. */
    public record PlannedEvent(FragmentType type, int version, long offsetUs, boolean duplicate, boolean stale) {
    }

    public record AggregatePlan(long index, String correlationId, int partition, long startUs, boolean warmup,
                                List<PlannedEvent> events, int sentMask, int missingType, int duplicateType,
                                int staleType, boolean outOfOrder, boolean requiresInference) {
    }

    private final Settings s;
    private final double aggregateRate;
    private final double rampSeconds;
    private final double rampAggregates;
    private final long totalAggregates;

    public WorkloadPlan(Settings settings) {
        this.s = settings;
        double meanEvents = 3 - settings.missingPercent() / 100 + settings.duplicatePercent() / 100 + settings.stalePercent() / 100;
        this.aggregateRate = settings.eventRate() / meanEvents;
        this.rampSeconds = Math.min(settings.rampSeconds(), settings.warmupSeconds() + settings.durationSeconds());
        this.rampAggregates = aggregateRate * rampSeconds * (1 + RAMP_START_FRACTION) / 2;
        this.totalAggregates = (long) Math.floor(aggregatesStartedBy(settings.warmupSeconds() + settings.durationSeconds()));
    }

    public long totalAggregates() {
        return totalAggregates;
    }

    public double aggregateRate() {
        return aggregateRate;
    }

    /** Requested fragment rate at {@code seconds} after start (ramp, then constant). */
    public double requestedEventRate(double seconds) {
        if (seconds < rampSeconds) {
            return s.eventRate() * (RAMP_START_FRACTION + (1 - RAMP_START_FRACTION) * seconds / rampSeconds);
        }
        return s.eventRate();
    }

    private double aggregatesStartedBy(double seconds) {
        if (seconds <= rampSeconds) {
            return aggregateRate * (RAMP_START_FRACTION * seconds + (1 - RAMP_START_FRACTION) * seconds * seconds / (2 * rampSeconds));
        }
        return rampAggregates + aggregateRate * (seconds - rampSeconds);
    }

    /** Planned start of aggregate {@code index}, in µs after the run start. */
    public long startUs(long index) {
        double seconds;
        if (index < rampAggregates && rampSeconds > 0) {
            double a = (1 - RAMP_START_FRACTION) / (2 * rampSeconds);
            double b = RAMP_START_FRACTION;
            double c = -index / aggregateRate;
            seconds = (-b + Math.sqrt(b * b - 4 * a * c)) / (2 * a);
        } else {
            seconds = rampSeconds + (index - rampAggregates) / aggregateRate;
        }
        return (long) (seconds * 1_000_000);
    }

    public AggregatePlan plan(long index) {
        SplittableRandom rng = new SplittableRandom(mix(s.seed(), index));
        String correlationId = correlationId(index, rng);
        int partition = partitionFor(correlationId, s.partitions());
        boolean requiresInference = rng.nextDouble() * 100 < s.inferencePercent();
        int missingType = rng.nextDouble() * 100 < s.missingPercent() ? 1 + rng.nextInt(3) : 0;

        long windowUs = s.correlationWindowMs() * 1000;
        long[] times = switch (s.windowDistribution()) {
            case "fixed" -> new long[]{0, windowUs / 2, windowUs};
            case "exponential" -> {
                long g1 = (long) (-Math.log(1 - rng.nextDouble()) * windowUs / 2);
                long g2 = (long) (-Math.log(1 - rng.nextDouble()) * windowUs / 2);
                yield new long[]{0, g1, g1 + g2};
            }
            default -> {
                long t1 = windowUs > 0 ? rng.nextLong(windowUs + 1) : 0;
                long t2 = windowUs > 0 ? rng.nextLong(windowUs + 1) : 0;
                yield new long[]{0, Math.min(t1, t2), Math.max(t1, t2)};
            }
        };
        FragmentType[] order = {FragmentType.A, FragmentType.B, FragmentType.C};
        boolean outOfOrder = rng.nextDouble() * 100 < s.outOfOrderPercent();
        if (outOfOrder) {
            FragmentType[][] others = {{FragmentType.A, FragmentType.C, FragmentType.B}, {FragmentType.B, FragmentType.A, FragmentType.C},
                    {FragmentType.B, FragmentType.C, FragmentType.A}, {FragmentType.C, FragmentType.A, FragmentType.B},
                    {FragmentType.C, FragmentType.B, FragmentType.A}};
            order = others[rng.nextInt(others.length)];
        }

        List<PlannedEvent> events = new ArrayList<>(5);
        List<PlannedEvent> originals = new ArrayList<>(3);
        int sentMask = 0;
        for (int k = 0; k < 3; k++) {
            if (order[k].code() == missingType) {
                continue;
            }
            PlannedEvent event = new PlannedEvent(order[k], 1, times[k], false, false);
            originals.add(event);
            events.add(event);
            sentMask |= order[k].bit();
        }
        int duplicateType = 0;
        if (!originals.isEmpty() && rng.nextDouble() * 100 < s.duplicatePercent()) {
            PlannedEvent original = originals.get(rng.nextInt(originals.size()));
            duplicateType = original.type().code();
            long delay = 1 + (s.duplicateDelayMaxMs() > 0 ? rng.nextLong(s.duplicateDelayMaxMs() * 1000) : 0);
            events.add(new PlannedEvent(original.type(), 1, original.offsetUs() + delay, true, false));
        }
        int staleType = 0;
        if (!originals.isEmpty() && rng.nextDouble() * 100 < s.stalePercent()) {
            PlannedEvent original = originals.get(rng.nextInt(originals.size()));
            staleType = original.type().code();
            long delay = 1 + (s.duplicateDelayMaxMs() > 0 ? rng.nextLong(s.duplicateDelayMaxMs() * 1000) : 0);
            events.add(new PlannedEvent(original.type(), 0, original.offsetUs() + delay, false, true));
        }
        events.sort(Comparator.comparingLong(PlannedEvent::offsetUs));

        long startUs = startUs(index);
        return new AggregatePlan(index, correlationId, partition, startUs, startUs < s.warmupSeconds() * 1_000_000L,
                List.copyOf(events), sentMask, missingType, duplicateType, staleType, outOfOrder, requiresInference);
    }

    public String payload(long index, FragmentType type, int version) {
        int size = switch (type) {
            case A -> s.payloadSizeA();
            case B -> s.payloadSizeB();
            case C -> s.payloadSizeC();
        };
        SplittableRandom rng = new SplittableRandom(mix(mix(s.seed(), index), type.code() * 31L + version));
        char[] chars = new char[size];
        for (int i = 0; i < size; i++) {
            chars[i] = ALPHABET[rng.nextInt(ALPHABET.length)];
        }
        return new String(chars);
    }

    public String payloadHash(long index, FragmentType type, int version) {
        return Hashing.sha256(payload(index, type, version));
    }

    public FragmentEvent event(String runId, AggregatePlan plan, PlannedEvent event, long generatedAtUs) {
        return new FragmentEvent(runId, plan.correlationId(),
                FragmentEvent.eventId(plan.correlationId(), event.type(), event.version()),
                event.type().code(), event.version(), plan.requiresInference(),
                payload(plan.index(), event.type(), event.version()), generatedAtUs);
    }

    private String correlationId(long index, SplittableRandom rng) {
        String base = "k" + Long.toString(index, 36);
        if (s.hotPartitionFraction() <= 0) {
            return base;
        }
        int hotPartitions = Math.max(1, (int) Math.round(s.partitions() * s.hotPartitionFraction()));
        boolean hot = rng.nextDouble() < s.hotTrafficFraction();
        for (int attempt = 0; attempt < 10_000; attempt++) {
            String candidate = base + "-" + attempt;
            boolean inHot = partitionFor(candidate, s.partitions()) < hotPartitions;
            if (inHot == hot) {
                return candidate;
            }
        }
        return base;
    }

    /** Kafka's default partitioner for keyed records. */
    public static int partitionFor(String key, int partitions) {
        return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % partitions;
    }

    private static long mix(long a, long b) {
        long z = a * 0x9E3779B97F4A7C15L + b;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
