package com.example.streams;

import com.example.common.Trade;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Consumidor usando KAFKA STREAMS.
 *
 * Faz EXATAMENTE a mesma agregacao do consumer-standard (preco medio, VWAP,
 * min, max e volume por papel em janelas de 10s), mas de forma declarativa.
 *
 * Repare que TODA a logica cabe em uma unica topologia:
 *   stream -> groupByKey -> windowedBy(10s) -> aggregate -> suppress -> foreach
 *
 * De brinde ganhamos (sem escrever codigo extra):
 *   - janela por EVENT-TIME (usa o timestamp do registro, nao o relogio)
 *   - estado tolerante a falhas (persistido em topico interno / state store)
 *   - "final result" por janela via suppress(untilWindowCloses)
 */
public class StreamsConsumerApp {

    private static final String TOPIC = "stock-trades";
    private static final DateTimeFormatter HHMMSS =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    public static void main(String[] args) {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        JsonSerde<Trade> tradeSerde = new JsonSerde<>(Trade.class);
        JsonSerde<WindowStats> statsSerde = new JsonSerde<>(WindowStats.class);

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(TOPIC, Consumed.with(Serdes.String(), tradeSerde))
                .groupByKey(Grouped.with(Serdes.String(), tradeSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                .aggregate(
                        WindowStats::empty,
                        (ticker, trade, stats) -> stats.add(trade),
                        Materialized.<String, WindowStats, WindowStore<Bytes, byte[]>>as("stock-window-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(statsSerde))
                // emite apenas o resultado FINAL de cada janela (quando ela fecha)
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .foreach(StreamsConsumerApp::printWindow);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[STREAMS] Encerrando...");
            streams.close();
            latch.countDown();
        }));

        System.out.println("[STREAMS] Iniciando topologia (broker: " + bootstrap + ")");
        System.out.println("[STREAMS] Janelas de 10s por event-time, resultado final via suppress\n");

        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void printWindow(Windowed<String> key, WindowStats stats) {
        Instant start = key.window().startTime();
        Instant end = key.window().endTime();
        System.out.printf(
                "[STREAMS] %-6s | janela %s-%s | trades=%d | precoMed=R$%.2f | vwap=R$%.2f | min=R$%.2f | max=R$%.2f | volume=%d%n",
                key.key(), HHMMSS.format(start), HHMMSS.format(end),
                stats.count(), stats.avgPrice(), stats.vwap(), stats.min(), stats.max(), stats.sumVolume());
    }
}
