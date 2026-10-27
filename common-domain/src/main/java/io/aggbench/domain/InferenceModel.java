package io.aggbench.domain;

/**
 * The deterministic "model" behind the KServe-compatible ML simulator.
 * Features are derived from the aggregate checksum, so the verifier can recompute the expected score
 * for every persisted aggregate without trusting the implementation under test.
 */
public final class InferenceModel {

    public static final String MODEL_NAME = "aggregate-scorer";
    public static final int FEATURES = 8;

    private static final double[] WEIGHTS = {0.8, -1.2, 0.5, 1.5, -0.7, 0.3, -0.4, 0.9};
    private static final double BIAS = -0.35;

    private InferenceModel() {
    }

    public static double[] features(String aggregateChecksum) {
        double[] features = new double[FEATURES];
        for (int i = 0; i < FEATURES; i++) {
            features[i] = Integer.parseInt(aggregateChecksum.substring(i * 2, i * 2 + 2), 16) / 255.0;
        }
        return features;
    }

    public static double score(double[] features) {
        double sum = BIAS;
        for (int i = 0; i < FEATURES; i++) {
            sum += WEIGHTS[i] * features[i];
        }
        double score = 1.0 / (1.0 + Math.exp(-sum));
        return Math.round(score * 1_000_000d) / 1_000_000d;
    }
}
