package io.aggbench.domain;

/**
 * Where and when a fragment was appended to the ingress topic.
 *
 * @param partition    ingress partition
 * @param offset       ingress offset
 * @param appendTimeMs broker append timestamp (the ingress topic uses {@code message.timestamp.type=LogAppendTime})
 */
public record IngressPosition(int partition, long offset, long appendTimeMs) {

    /** Identity of the Kafka record; used to tell a crash redelivery from a logical duplicate. */
    public String recordRef() {
        return partition + ":" + offset;
    }
}
