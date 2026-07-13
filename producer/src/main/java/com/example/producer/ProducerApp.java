package com.example.producer;

import com.example.common.Json;
import com.example.common.Transaction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * ============================================================================
 *  PRODUCER (lib padrao / KafkaProducer)
 * ============================================================================
 *
 * Simula um sistema de pagamentos gerando transacoes financeiras:
 *   - 5 contas (ACC-001 a ACC-005)
 *   - a maioria das transacoes e normal (R$ 50 a R$ 5.000)
 *   - ~1 em cada 5 e uma transacao ALTA (acima de R$ 10.000) -> vai disparar
 *     o alerta nos dois consumidores
 *
 * A CHAVE da mensagem e o accountId. Isso garante que todas as transacoes de
 * uma mesma conta caiam na mesma particao (mantendo a ordem) e permite que os
 * consumidores agrupem por conta.
 *
 * Repare: produzir mensagem e IGUAL nas duas abordagens. A diferenca entre a
 * lib padrao e o Kafka Streams aparece no CONSUMO.
 */
public class ProducerApp {

    private static final String TOPIC = "transactions";
    private static final List<String> ACCOUNTS =
            List.of("ACC-001", "ACC-002", "ACC-003", "ACC-004", "ACC-005");

    public static void main(String[] args) throws InterruptedException {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[PRODUCER] Encerrando...");
            producer.flush();
            producer.close();
        }));

        System.out.println("[PRODUCER] Publicando transacoes no topico '" + TOPIC + "' (broker: " + bootstrap + ")\n");

        int counter = 1;
        while (true) {
            var random = ThreadLocalRandom.current();

            String accountId = ACCOUNTS.get(random.nextInt(ACCOUNTS.size()));

            // 1 em cada 5 transacoes e alta (acima de R$ 10.000)
            boolean alta = random.nextInt(5) == 0;
            double amount = alta
                    ? random.nextDouble(10_001, 50_000)
                    : random.nextDouble(50, 5_000);
            amount = Math.round(amount * 100.0) / 100.0;

            Transaction tx = new Transaction(
                    String.format("TX-%04d", counter++),
                    accountId,
                    amount,
                    System.currentTimeMillis());

            // chave = accountId  |  valor = a transacao em JSON
            producer.send(new ProducerRecord<>(TOPIC, accountId, Json.toJson(tx)));

            System.out.printf("[PRODUCER] %s | %s | R$ %10.2f %s%n",
                    tx.id(), tx.accountId(), tx.amount(), alta ? "<- ALTA" : "");

            Thread.sleep(1000); // 1 transacao por segundo, para dar para acompanhar
        }
    }
}
