package com.example.producer;

import com.example.common.Json;
import com.example.common.Trade;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Gera um fluxo continuo de negociacoes (trades) de acoes da B3 e publica
 * no topico "stock-trades". Usa a lib padrao (kafka-clients / KafkaProducer).
 *
 * A chave da mensagem e o ticker da acao - assim todas as negociacoes de um
 * mesmo papel caem na mesma particao e mantem ordem por papel.
 */
public class ProducerApp {

    private static final String TOPIC = "stock-trades";

    /** Precos base (em R$) usados como ponto de partida do random walk. */
    private static final Map<String, Double> BASE_PRICES = new LinkedHashMap<>() {{
        put("PETR4", 38.50);
        put("VALE3", 61.20);
        put("ITUB4", 33.80);
        put("BBDC4", 14.70);
        put("MGLU3", 12.40);
        put("WEGE3", 39.90);
        put("ABEV3", 13.10);
    }};

    public static void main(String[] args) throws InterruptedException {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        // estado local: preco atual de cada papel (evolui em random walk)
        Map<String, Double> currentPrices = new LinkedHashMap<>(BASE_PRICES);
        List<String> tickers = List.copyOf(BASE_PRICES.keySet());
        Random random = new Random();

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[PRODUCER] Encerrando...");
            producer.flush();
            producer.close();
        }));

        System.out.println("[PRODUCER] Publicando trades no topico '" + TOPIC + "' (broker: " + bootstrap + ")");

        while (true) {
            String ticker = tickers.get(random.nextInt(tickers.size()));

            // random walk: varia +-0.6% em torno do preco atual
            double price = currentPrices.get(ticker);
            price = price * (1 + (random.nextDouble() - 0.5) * 0.012);
            price = Math.max(0.5, Math.round(price * 100.0) / 100.0);
            currentPrices.put(ticker, price);

            int quantity = ThreadLocalRandom.current().nextInt(100, 5_000);
            Trade trade = new Trade(ticker, price, quantity, System.currentTimeMillis());

            producer.send(new ProducerRecord<>(TOPIC, ticker, Json.toJson(trade)), (metadata, ex) -> {
                if (ex != null) {
                    System.err.println("[PRODUCER] Erro ao enviar: " + ex.getMessage());
                }
            });

            System.out.printf("[PRODUCER] %-6s | R$ %8.2f | qtd %5d%n", ticker, price, quantity);

            // ~4 trades por segundo
            Thread.sleep(ThreadLocalRandom.current().nextLong(150, 400));
        }
    }
}
