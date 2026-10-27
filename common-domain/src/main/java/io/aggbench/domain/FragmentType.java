package io.aggbench.domain;

/**
 * The three fragment types that together form one business aggregate.
 * The numeric code is the {@code eventType} on the wire; the bit is used in fragment masks
 * (A=1, B=2, C=4, so a complete aggregate has mask 7).
 */
public enum FragmentType {
    A(1),
    B(2),
    C(3);

    public static final int COMPLETE_MASK = 0b111;
    public static final int STAGE2_MASK = 0b011;

    private final int code;

    FragmentType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public int bit() {
        return 1 << (code - 1);
    }

    public static FragmentType of(int code) {
        return switch (code) {
            case 1 -> A;
            case 2 -> B;
            case 3 -> C;
            default -> throw new IllegalArgumentException("Unknown fragment type " + code);
        };
    }
}
