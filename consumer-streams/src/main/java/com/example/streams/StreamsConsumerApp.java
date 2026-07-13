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
 * ============================================================================
 *  CONSUMIDOR COM KAFKA STREAMS
 * ============================================================================
 *
 * Faz EXATAMENTE as mesmas duas regras do consumer-standard:
 *
 *   REGRA 1 - Alerta se a transacao for acima de R$ 10.000
 *   REGRA 2 - Contar as transacoes de cada conta nos ultimos 5 minutos
 *
 * A diferenca esta em COMO escrevemos isso.
 *
 * Nao existe loop de poll. Nao existe Map na memoria. Nao existe limpeza
 * manual do que expirou. Nos apenas DESCREVEMOS o que queremos (a "topologia")
 * e a biblioteca executa:
 *
 *   REGRA 1:  stream -> filter(valor > 10000) -> imprime
 *   REGRA 2:  stream -> groupByKey -> windowedBy(5 min) -> count -> imprime
 *
 * E o mais importante: na REGRA 2, o Kafka Streams guarda a contagem num
 * "State Store" com BACKUP AUTOMATICO em um topico interno do Kafka.
 * Se este processo reiniciar, ele RECUPERA a contagem e continua de onde
 * parou - coisa que o Map em memoria do consumer-standard nao consegue.
 */
public class StreamsConsumerApp {

    private static final String TOPIC = "transactions";
    private static final double LIMITE_ALERTA = 10_000.00;

    public static void main(String[] args) {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        Properties props = new Properties();
        // o "nome" da aplicacao: define o grupo de consumo e o prefixo dos
        // topicos internos onde o estado fica salvo
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "transactions-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // le o topico desde o inicio (assim a contagem bate com a do consumer-standard)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // desliga o buffer interno para o resultado aparecer na hora (bom p/ demo)
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        // Serde = SERializer + DESerializer: ensina o Streams a converter
        // os bytes do topico no nosso record Transaction (e vice-versa).
        JsonSerde<Transaction> txSerde = new JsonSerde<>(Transaction.class);

        StreamsBuilder builder = new StreamsBuilder();

        // O fluxo de transacoes, lido do topico. Este objeto e reaproveitado
        // pelas duas regras abaixo - o Kafka le o topico uma vez so.
        KStream<String, Transaction> transacoes =
                builder.stream(TOPIC, Consumed.with(Serdes.String(), txSerde));

        // ---------------------------------------------------------------------
        // REGRA 1: alerta de valor alto
        // "do fluxo, me de so as transacoes acima de 10 mil, e imprima"
        // ---------------------------------------------------------------------
        transacoes
                .filter((accountId, tx) -> tx.amount() > LIMITE_ALERTA)
                .foreach((accountId, tx) ->
                        System.out.printf("[STREAMS]  *** ALERTA *** %s | %s | R$ %.2f acima do limite!%n",
                                tx.id(), tx.accountId(), tx.amount()));

        // ---------------------------------------------------------------------
        // REGRA 2: contagem por conta nos ultimos 5 minutos
        // "agrupe por conta, janele de 5 em 5 minutos, conte"
        //
        // Compare com o consumer-standard: la foram um Map, um Deque e uma
        // limpeza manual do que expirou. Aqui sao 3 linhas - e com estado
        // tolerante a falhas de brinde.
        // ---------------------------------------------------------------------
        transacoes
                .groupByKey(Grouped.with(Serdes.String(), txSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .count()
                .toStream()
                .foreach(StreamsConsumerApp::imprimirContagem);

        // ---------------------------------------------------------------------
        // Liga o motor e trata o desligamento limpo.
        // ---------------------------------------------------------------------
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

    /** A chave aqui e "conta + janela de tempo", e o valor e a contagem. */
    private static void imprimirContagem(Windowed<String> chave, Long quantidade) {
        System.out.printf("[STREAMS]  %s | %d transacao(oes) na janela de 5 min%n",
                chave.key(), quantidade);
    }
}
