package io.aggbench.execution;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OffsetTrackerTest {

    private static final TopicPartition P0 = new TopicPartition("t", 0);

    @Test
    void commitsOnlyUpToFirstUnfinishedRecord() {
        OffsetTracker tracker = new OffsetTracker();
        for (long offset = 10; offset < 15; offset++) {
            tracker.register(P0, offset);
        }
        tracker.complete(P0, 10);
        tracker.complete(P0, 12);
        tracker.complete(P0, 13);
        assertEquals(Map.of(P0, new OffsetAndMetadata(11)), tracker.committable());

        tracker.complete(P0, 11);
        assertEquals(Map.of(P0, new OffsetAndMetadata(14)), tracker.committable());

        tracker.complete(P0, 14);
        assertEquals(Map.of(P0, new OffsetAndMetadata(15)), tracker.committable());
        assertTrue(tracker.committable().isEmpty(), "nothing new to commit");
        assertEquals(0, tracker.inFlight());
    }

    @Test
    void replayResetNeverCommitsBackwards() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.register(P0, 100);
        tracker.complete(P0, 100);
        assertEquals(101, tracker.committable().get(P0).offset());

        tracker.reset(List.of(P0));
        tracker.register(P0, 50);
        tracker.complete(P0, 50);
        assertTrue(tracker.committable().isEmpty(), "replayed position 51 is behind committed 101");

        for (long offset = 51; offset <= 120; offset++) {
            tracker.register(P0, offset);
            tracker.complete(P0, offset);
        }
        assertEquals(121, tracker.committable().get(P0).offset());
    }

    @Test
    void idlePartitionCommitsFetchPositionPastTransactionMarker() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.register(P0, 10);
        assertEquals(10, tracker.committable(p -> 12).get(P0).offset(), "work pending: commit the pending offset");
        tracker.complete(P0, 10);
        assertEquals(12, tracker.committable(p -> 12).get(P0).offset(), "idle: commit past the commit marker");
    }

    @Test
    void revokedPartitionsIgnoreLateCompletions() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.register(P0, 1);
        tracker.remove(List.of(P0));
        tracker.complete(P0, 1);
        assertTrue(tracker.committable().isEmpty());
    }
}
