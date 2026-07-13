package com.example.standard;

import com.example.common.Json;
import com.example.common.Trade;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Consumidor usando a LIB PADRAO (kafka-clients / KafkaConsumer).
 *
 * Objetivo didatico: mostrar quanto codigo "na mao" e preciso para calcular
 * uma agregacao por janela de tempo (preco medio, VWAP, min, max e volume por
 * papel a cada 10s). Repare que precisamos gerenciar:
 *   - o loop de poll
 *   - o estado em memoria (o Map de acumuladores)
 *   - o disparo temporal da janela (um scheduler separado)
 *   - a sincronizacao entre a thread de poll e a thread do scheduler
 *
 * Esta janela e por RELOGIO (processing-time) e NAO sobrevive a um restart:
 * se o processo cair, o estado acumulado se perde. Compare com o Kafka Streams.
 */
public class StandardConsumerApp {

    private static final String TOPIC = "stock-trades";
    private static final long WINDOW_SECONDS = 10;

    // estado da janela atual: acumulador por ticker
    private static final Map<String, Acc> window = new HashMap<>();
    private static final Object lock = new Object();

    private static volatile boolean running = true;

    public static void main(String[] args) {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "standard-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(TOPIC));

        // thread separada so para "fechar" a janela a cada 10 segundos
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(StandardConsumerApp::flushWindow,
                WINDOW_SECONDS, WINDOW_SECONDS, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            consumer.wakeup();
        }));

        System.out.println("[STANDARD] Consumindo '" + TOPIC + "' (broker: " + bootstrap + ")");
        System.out.println("[STANDARD] Agregacao manual a cada " + WINDOW_SECONDS + "s\n");

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    Trade trade = Json.fromJson(record.value(), Trade.class);
                    synchronized (lock) {
                        window.computeIfAbsent(trade.ticker(), k -> new Acc()).add(trade);
                    }
                }
            }
        } catch (WakeupException e) {
            // esperado no shutdown
        } finally {
            consumer.close();
            scheduler.shutdown();
            System.out.println("[STANDARD] Encerrado.");
        }
    }

    /** "Fecha" a janela: tira um snapshot do estado, zera e imprime o resultado. */
    private static void flushWindow() {
        Map<String, Acc> snapshot;
        synchronized (lock) {
            snapshot = new TreeMap<>(window);
            window.clear();
        }

        if (snapshot.isEmpty()) {
            System.out.println("[STANDARD] (janela sem trades)");
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\n[STANDARD] ===== Janela de ").append(WINDOW_SECONDS).append("s (calculada na mao) =====\n");
        sb.append(String.format("%-7s | %6s | %10s | %10s | %10s | %10s | %10s%n",
                "TICKER", "TRADES", "PRECO MED", "VWAP", "MIN", "MAX", "VOLUME"));
        snapshot.forEach((ticker, acc) ->
                sb.append(String.format("%-7s | %6d | %10.2f | %10.2f | %10.2f | %10.2f | %10d%n",
                        ticker, acc.count, acc.avgPrice(), acc.vwap(), acc.min, acc.max, acc.sumVolume)));
        System.out.print(sb);
    }

    /** Acumulador manual das estatisticas de um papel dentro da janela. */
    private static final class Acc {
        long count;
        double sumPrice;
        double sumPriceVolume;
        long sumVolume;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;

        void add(Trade t) {
            count++;
            sumPrice += t.price();
            sumPriceVolume += t.price() * t.quantity();
            sumVolume += t.quantity();
            min = Math.min(min, t.price());
            max = Math.max(max, t.price());
        }

        double avgPrice() {
            return count == 0 ? 0 : sumPrice / count;
        }

        double vwap() {
            return sumVolume == 0 ? 0 : sumPriceVolume / sumVolume;
        }
    }
}
