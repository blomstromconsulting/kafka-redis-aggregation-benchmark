package io.aggbench.domain;

/**
 * Result classification for one fragment delivery. Both implementations (Java for Kafka Streams,
 * Lua for Redis) must classify every delivery identically.
 */
public enum MergeOutcome {
    /** First fragment of this type: stored. Individual action due; stages may trigger. */
    ACCEPTED(true),
    /** Higher version replacing a fragment no emitted stage has consumed yet. Individual action due. */
    SUPERSEDED(true),
    /** The exact same Kafka record seen again (crash before offset commit). Re-issue the same actions. */
    REDELIVERED(true),
    /** Higher version arriving after an emitted stage consumed the type. Individual action due, aggregate unchanged. */
    LATE_VERSION(true),
    /** Same event id, different Kafka record: a logical duplicate. Ignored. */
    DUPLICATE(false),
    /** Older version than the stored one. Ignored. */
    STALE(false);

    private final boolean individualActionDue;

    MergeOutcome(boolean individualActionDue) {
        this.individualActionDue = individualActionDue;
    }

    public boolean individualActionDue() {
        return individualActionDue;
    }
}
