package io.aggbench.domain;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Content hashes used in action receipts and persisted aggregates. The verifier recomputes the same
 * hashes from the load generator's manifest, so these formulas are part of the benchmark contract.
 */
public final class Hashing {

    private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    });

    private Hashing() {
    }

    public static String sha256(String text) {
        MessageDigest digest = SHA256.get();
        digest.reset();
        return HexFormat.of().formatHex(digest.digest(text.getBytes(StandardCharsets.UTF_8)));
    }

    /** Hash of the cumulative A+B representation. */
    public static String cumulativeHash(String eventIdA, String hashA, String eventIdB, String hashB) {
        return sha256("A=" + eventIdA + "#" + hashA + "|B=" + eventIdB + "#" + hashB);
    }

    /** Checksum of the complete A+B+C aggregate. */
    public static String aggregateChecksum(String eventIdA, String hashA, String eventIdB, String hashB,
                                           String eventIdC, String hashC) {
        return sha256("A=" + eventIdA + "#" + hashA + "|B=" + eventIdB + "#" + hashB + "|C=" + eventIdC + "#" + hashC);
    }
}
