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
 * ============================================================================
 *  CONSUMIDOR COM A LIB PADRAO (kafka-clients / KafkaConsumer)
 * ============================================================================
 *
 * Faz DUAS coisas simples:
 *
 *   REGRA 1 - Alerta de valor alto:
 *             se a transacao for acima de R$ 10.000, imprime um ALERTA.
 *
 *   REGRA 2 - Contagem por conta:
 *             quantas transacoes aquela conta fez na janela de 5 minutos.
 *
 * A REGRA 1 e facil: e so um "if" na mensagem que acabou de chegar.
 * Nao preciso lembrar de nada. Aqui a lib padrao e perfeita.
 *
 * A REGRA 2 e o problema: para contar, eu preciso LEMBRAR das transacoes
 * anteriores. Ou seja, preciso de ESTADO. E a lib padrao nao me da estado
 * nenhum - eu tenho que:
 *
 *   1. criar um Map na memoria para guardar o contador de cada conta
 *   2. descobrir na mao em qual janela de 5 minutos a transacao cai
 *   3. zerar o contador na mao quando a janela virar
 *   4. conviver com o fato de que, SE ESTE PROCESSO REINICIAR,
 *      esse Map se PERDE e a contagem volta do zero.
 *
 * Guarde esse contraste: e exatamente isso que o Kafka Streams resolve.
 */
public class StandardConsumerApp {

    private static final String TOPIC = "transactions";
    private static final double LIMITE_ALERTA = 10_000.00;
    private static final long JANELA_MS = Duration.ofMinutes(5).toMillis();

    /**
     * O ESTADO, mantido na mao: o contador de cada conta.
     * Vive apenas na memoria deste processo - reiniciou, perdeu.
     */
    private static final Map<String, Contador> contadores = new HashMap<>();

    public static void main(String[] args) {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "standard-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // le o topico desde o inicio (assim a contagem bate com a do consumer-streams)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(TOPIC));

        System.out.println("[STANDARD] Consumindo '" + TOPIC + "' (broker: " + bootstrap + ")");
        System.out.println("[STANDARD] Regra 1: alerta acima de R$ " + LIMITE_ALERTA);
        System.out.println("[STANDARD] Regra 2: contagem por conta em janelas de 5 min\n");

        // O LOOP DE POLL: eu que busco as mensagens, eu que trato uma por uma.
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, String> record : records) {
                Transaction tx = Json.fromJson(record.value(), Transaction.class);

                // ----------------------------------------------------------
                // REGRA 1: valor alto? Basta olhar a mensagem atual. Simples.
                // ----------------------------------------------------------
                if (tx.amount() > LIMITE_ALERTA) {
                    System.out.printf("[STANDARD] *** ALERTA *** %s | %s | R$ %.2f acima do limite!%n",
                            tx.id(), tx.accountId(), tx.amount());
                }

                // ----------------------------------------------------------
                // REGRA 2: contar por conta na janela de 5 min.
                //          Aqui eu preciso de ESTADO - e mante-lo e por minha conta.
                // ----------------------------------------------------------
                int quantidade = contarNaJanela(tx);

                System.out.printf("[STANDARD] %s | %s | R$ %10.2f | %d transacao(oes) na janela de 5 min%n",
                        tx.id(), tx.accountId(), tx.amount(), quantidade);
            }
        }
    }

    /**
     * Conta as transacoes da conta dentro da janela de 5 minutos.
     *
     * Repare em TODO o trabalho manual que a lib padrao me obriga a fazer:
     *   - descobrir em qual bloco de 5 minutos esta transacao caiu
     *   - guardar o contador dessa conta no Map
     *   - perceber que a janela virou e zerar o contador
     *
     * No consumer-streams isso tudo e UMA linha: .windowedBy(...).count()
     */
    private static int contarNaJanela(Transaction tx) {
        // em qual bloco de 5 minutos esta transacao caiu?
        long janelaAtual = tx.timestamp() / JANELA_MS;

        Contador contador = contadores.computeIfAbsent(tx.accountId(), k -> new Contador());

        // a janela virou? entao zera o contador e comeca a contar de novo
        if (contador.janela != janelaAtual) {
            contador.janela = janelaAtual;
            contador.quantidade = 0;
        }

        contador.quantidade++;
        return contador.quantidade;
    }

    /** O contador de UMA conta: em qual janela ela esta e quantas transacoes fez nela. */
    private static final class Contador {
        long janela = -1;
        int quantidade;
    }
}
