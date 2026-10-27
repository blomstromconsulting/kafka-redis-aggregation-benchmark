package io.aggbench.streams;

import io.aggbench.domain.Json;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** Jackson-based serde for record values and state store values. */
public final class JsonSerde<T> implements Serde<T> {

    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, value) -> value == null ? null : Json.toBytes(value);
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> bytes == null ? null : Json.fromBytes(bytes, type);
    }
}
