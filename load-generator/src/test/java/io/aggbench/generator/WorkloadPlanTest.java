package io.aggbench.generator;

import io.aggbench.domain.FragmentType;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkloadPlanTest {

    private static WorkloadPlan.Settings settings(long seed) {
        return new WorkloadPlan.Settings(seed, 6000, 30, 15, 60, 200, 200, 400, 1000, "uniform",
                10, 2, 2000, 1, 1, 100, 12, 0, 0);
    }

    @Test
    void sameSeedYieldsIdenticalWorkload() {
        WorkloadPlan first = new WorkloadPlan(settings(7));
        WorkloadPlan second = new WorkloadPlan(settings(7));
        assertEquals(first.totalAggregates(), second.totalAggregates());
        for (long i = 0; i < 2000; i += 37) {
            assertEquals(first.plan(i), second.plan(i));
            assertEquals(first.payload(i, FragmentType.C, 1), second.payload(i, FragmentType.C, 1));
        }
    }

    @Test
    void offeredFragmentRateMatchesRequestedRateAfterRamp() {
        WorkloadPlan plan = new WorkloadPlan(settings(1));
        long events = 0;
        long aggregates = 0;
        long fromUs = 40_000_000L;
        long toUs = 70_000_000L;
        for (long i = 0; i < plan.totalAggregates(); i++) {
            long start = plan.startUs(i);
            if (start >= fromUs && start < toUs) {
                events += plan.plan(i).events().size();
                aggregates++;
            }
        }
        double rate = events / 30.0;
        assertTrue(Math.abs(rate - 6000) / 6000 < 0.02, "offered rate " + rate);
        assertTrue(aggregates > 0);
    }

    @Test
    void startTimesAreMonotonicAcrossRamp() {
        WorkloadPlan plan = new WorkloadPlan(settings(3));
        long previous = -1;
        for (long i = 0; i < plan.totalAggregates(); i += 101) {
            long start = plan.startUs(i);
            assertTrue(start >= previous);
            previous = start;
        }
        assertTrue(plan.startUs(plan.totalAggregates() - 1) <= 90_000_000L);
    }

    @Test
    void staleAndDuplicateEventsFollowTheirOriginals() {
        WorkloadPlan plan = new WorkloadPlan(new WorkloadPlan.Settings(5, 1000, 0, 0, 60, 10, 10, 10, 500, "uniform",
                50, 50, 1000, 10, 50, 50, 12, 0, 0));
        for (long i = 0; i < 1000; i++) {
            WorkloadPlan.AggregatePlan p = plan.plan(i);
            for (WorkloadPlan.PlannedEvent e : p.events()) {
                if (e.duplicate() || e.stale()) {
                    long originalOffset = p.events().stream()
                            .filter(o -> !o.duplicate() && !o.stale() && o.type() == e.type())
                            .findFirst().orElseThrow().offsetUs();
                    assertTrue(e.offsetUs() > originalOffset);
                }
            }
            if (p.missingType() != 0) {
                assertEquals(0, p.sentMask() & FragmentType.of(p.missingType()).bit());
            }
        }
    }

    @Test
    void hotPartitionSkewConcentratesTraffic() {
        WorkloadPlan plan = new WorkloadPlan(new WorkloadPlan.Settings(9, 1000, 0, 0, 60, 10, 10, 10, 500, "uniform",
                0, 0, 0, 0, 0, 0, 12, 0.25, 0.8));
        int hot = 0;
        Set<String> ids = new HashSet<>();
        for (long i = 0; i < 5000; i++) {
            WorkloadPlan.AggregatePlan p = plan.plan(i);
            ids.add(p.correlationId());
            if (p.partition() < 3) {
                hot++;
            }
        }
        assertEquals(5000, ids.size(), "correlation ids are unique");
        assertTrue(hot > 3700 && hot < 4300, "about 80% of aggregates on 3 of 12 partitions: " + hot);
    }
}
