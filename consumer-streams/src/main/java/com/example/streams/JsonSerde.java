package com.example.streams;

import com.example.common.Json;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serde = SERializer + DESerializer.
 *
 * O Kafka trafega bytes. Este "tradutor" ensina o Kafka Streams a converter
 * os bytes do topico no nosso record Transaction (e vice-versa), usando JSON.
 *
 * Na lib padrao o equivalente e o StringDeserializer + a chamada manual ao
 * Json.fromJson(...) dentro do loop.
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
