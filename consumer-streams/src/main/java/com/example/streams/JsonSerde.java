package com.example.streams;

import com.example.common.Json;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serde (serializer + deserializer) JSON: ensina o Kafka Streams a converter
 * os bytes do topico no record Transaction, e vice-versa.
 */
public class JsonSerde<T> implements Serde<T> {

    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> data == null ? null : Json.toBytes(data);
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> bytes == null ? null : Json.fromBytes(bytes, type);
    }
}
