package com.example.standard;

import com.example.common.Json;
import com.example.common.Transaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Consumidor com a lib padrao (KafkaConsumer).
 *
 * Regra 1 - alerta em transacoes acima de R$ 10.000 (sem estado).
 * Regra 2 - contagem por conta em janelas de 5 minutos (com estado, mantido na mao).
 */
public class StandardConsumerApp {

    private static final String TOPIC = "transactions";
    private static final double LIMITE_ALERTA = 10_000.00;
    private static final long JANELA_MS = Duration.ofMinutes(5).toMillis();

    /** O estado, mantido na mao: vive so na memoria deste processo. */
    private static final Map<String, Contador> contadores = new HashMap<>();

    public static void main(String[] args) {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "standard-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // desde o inicio do topico, para a contagem bater com a do consumer-streams
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        System.out.println("[STANDARD] Consumindo '" + TOPIC + "' (broker: " + bootstrap + ")");
        System.out.println("[STANDARD] Regra 1: alerta acima de R$ " + LIMITE_ALERTA);
        System.out.println("[STANDARD] Regra 2: contagem por conta em janelas de 5 min\n");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    Transaction tx = Json.fromJson(record.value(), Transaction.class);

                    // Regra 1: basta olhar a mensagem atual
                    if (tx.amount() > LIMITE_ALERTA) {
                        System.out.printf("[STANDARD] *** ALERTA *** %s | %s | R$ %.2f acima do limite!%n",
                                tx.id(), tx.accountId(), tx.amount());
                    }

                    // Regra 2: precisa lembrar das anteriores
                    int quantidade = contarNaJanela(tx);

                    System.out.printf("[STANDARD] %s | %s | R$ %10.2f | %d transacao(oes) na janela de 5 min%n",
                            tx.id(), tx.accountId(), tx.amount(), quantidade);
                }
            }
        }
    }

    private static int contarNaJanela(Transaction tx) {
        // em qual bloco de 5 minutos esta transacao caiu
        long janelaAtual = tx.timestamp() / JANELA_MS;

        Contador contador = contadores.computeIfAbsent(tx.accountId(), k -> new Contador());

        // a janela virou: zera o contador na mao
        if (contador.janela != janelaAtual) {
            contador.janela = janelaAtual;
            contador.quantidade = 0;
        }

        contador.quantidade++;
        return contador.quantidade;
    }

    private static final class Contador {
        long janela = -1;
        int quantidade;
    }
}
