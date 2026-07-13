package com.example.streams;

import com.example.common.Transaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Consumidor com Kafka Streams.
 *
 * Regra 1 - alerta em transacoes acima de R$ 10.000 (sem estado).
 * Regra 2 - contagem por conta em janelas de 5 minutos (estado gerenciado pelo Streams).
 */
public class StreamsConsumerApp {

    private static final String TOPIC = "transactions";
    private static final double LIMITE_ALERTA = 10_000.00;

    public static void main(String[] args) {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        Properties props = new Properties();
        // define o grupo de consumo e o prefixo dos topicos internos de estado
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "transactions-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // desde o inicio do topico, para a contagem bater com a do consumer-standard
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // sem buffer interno: o resultado aparece na hora
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        JsonSerde<Transaction> txSerde = new JsonSerde<>(Transaction.class);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Transaction> transacoes =
                builder.stream(TOPIC, Consumed.with(Serdes.String(), txSerde));

        // Regra 1
        transacoes
                .filter((accountId, tx) -> tx.amount() > LIMITE_ALERTA)
                .foreach((accountId, tx) ->
                        System.out.printf("[STREAMS]  *** ALERTA *** %s | %s | R$ %.2f acima do limite!%n",
                                tx.id(), tx.accountId(), tx.amount()));

        // Regra 2: o Streams cuida do estado e da janela
        transacoes
                .groupByKey(Grouped.with(Serdes.String(), txSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .count()
                .toStream()
                .foreach(StreamsConsumerApp::imprimirContagem);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[STREAMS] Encerrando...");
            streams.close();
            latch.countDown();
        }));

        System.out.println("[STREAMS] Iniciando topologia (broker: " + bootstrap + ")");
        System.out.println("[STREAMS] Regra 1: alerta acima de R$ " + LIMITE_ALERTA);
        System.out.println("[STREAMS] Regra 2: contagem por conta em janelas de 5 min\n");

        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void imprimirContagem(Windowed<String> chave, Long quantidade) {
        System.out.printf("[STREAMS]  %s | %d transacao(oes) na janela de 5 min%n",
                chave.key(), quantidade);
    }
}
