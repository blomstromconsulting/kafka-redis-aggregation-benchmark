package io.aggbench.redis;

import io.aggbench.domain.ActionIntent;
import io.aggbench.domain.ActionKind;
import io.aggbench.domain.AggregationStage;
import io.aggbench.domain.AggregationState;
import io.aggbench.domain.FragmentEvent;
import io.aggbench.domain.FragmentType;
import io.aggbench.domain.IngressPosition;
import io.aggbench.domain.MergeOutcome;
import io.aggbench.domain.MergeResult;
import io.aggbench.domain.PersistenceApi;
import io.aggbench.domain.StoredFragment;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Classes a native executable must reach reflectively; no effect on the JVM build. Jackson reads and writes the
 * domain records (Kafka values and HTTP bodies).
 */
@RegisterForReflection(targets = {ActionIntent.class, ActionKind.class, AggregationStage.class, AggregationState.class,
        AggregationState.SupersededFragment.class, FragmentEvent.class, FragmentType.class, IngressPosition.class,
        MergeOutcome.class, MergeResult.class, StoredFragment.class, PersistenceApi.Receipt.class,
        PersistenceApi.AggregateRecord.class, PersistenceApi.ItemStatus.class, PersistenceApi.BenchEvent.class})
final class NativeReflection {

    private NativeReflection() {
    }
}
