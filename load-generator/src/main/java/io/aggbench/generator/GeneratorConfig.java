package io.aggbench.generator;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/** Load generator configuration; every value is set by scripts/benchmark.sh through Helm. */
@ConfigMapping(prefix = "gen")
public interface GeneratorConfig {

    String runId();

    String variant();

    String scenario();

    @WithDefault("1")
    int repetition();

    String bootstrapServers();

    String ingressTopic();

    int partitions();

    String jdbcUrl();

    String jdbcUser();

    String jdbcPassword();

    @WithDefault("42")
    long seed();

    @WithDefault("100")
    double eventRate();

    @WithDefault("30")
    int warmupSeconds();

    @WithDefault("15")
    int rampSeconds();

    @WithDefault("60")
    int durationSeconds();

    @WithDefault("200")
    int payloadSizeA();

    @WithDefault("200")
    int payloadSizeB();

    @WithDefault("400")
    int payloadSizeC();

    @WithDefault("1000")
    long correlationWindowMs();

    /** uniform | fixed | exponential */
    @WithDefault("uniform")
    String windowDistribution();

    @WithDefault("10")
    double outOfOrderPercent();

    @WithDefault("2")
    double duplicatePercent();

    @WithDefault("2000")
    long duplicateDelayMaxMs();

    @WithDefault("1")
    double missingPercent();

    @WithDefault("0")
    double stalePercent();

    @WithDefault("100")
    double inferencePercent();

    @WithDefault("0")
    double hotPartitionFraction();

    @WithDefault("0")
    double hotTrafficFraction();

    /** Comma-separated {@code groupId:topic} pairs whose lag is sampled (the main group first). */
    String lagGroups();

    @WithDefault("1000")
    long lagSampleIntervalMs();

    @WithDefault("600")
    int drainTimeoutSeconds();

    /** Consecutive zero-lag samples required to call the system drained. */
    @WithDefault("5")
    int drainZeroSamples();

    default WorkloadPlan.Settings planSettings() {
        return new WorkloadPlan.Settings(seed(), eventRate(), warmupSeconds(), rampSeconds(), durationSeconds(),
                payloadSizeA(), payloadSizeB(), payloadSizeC(), correlationWindowMs(), windowDistribution(),
                outOfOrderPercent(), duplicatePercent(), duplicateDelayMaxMs(), missingPercent(), stalePercent(),
                inferencePercent(), partitions(), hotPartitionFraction(), hotTrafficFraction());
    }
}
