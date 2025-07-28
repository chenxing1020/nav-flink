package com.xchen.nav;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.xchen.nav.model.MarketData;
import com.xchen.nav.model.TradeOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class DataGenerator {

    public static final String KAFKA_BROKER = "localhost:9092";
    public static final String ORDERS_TOPIC = "trade_orders";
    public static final String MARKET_DATA_TOPIC = "market_data";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();
    private static final String[] SYMBOLS = {"AAPL", "GOOGL", "MSFT", "TSLA"};

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Start Trade Order Generator
        executor.submit(createProducer(ORDERS_TOPIC, () -> {
            TradeOrder order = new TradeOrder(
                    UUID.randomUUID().toString(),
                    "FUND-A", // A single fund for this demo
                    SYMBOLS[random.nextInt(SYMBOLS.length)],
                    (random.nextInt(10) + 1) * (random.nextBoolean() ? 1 : -1), // Buy or Sell 1-10 shares
                    System.currentTimeMillis()
            );
            return objectMapper.writeValueAsString(order);
        }, 2000)); // Slower trades

        // Start Market Data Generator
        executor.submit(createProducer(MARKET_DATA_TOPIC, () -> {
            MarketData data = new MarketData(
                    SYMBOLS[random.nextInt(SYMBOLS.length)],
                    // Price between 100 and 500
                    Math.round((100 + random.nextDouble() * 400) * 100.0) / 100.0,
                    System.currentTimeMillis()
            );
            return objectMapper.writeValueAsString(data);
        }, 500)); // Faster price updates

        log.info("Starting data generators for topics '{}' and '{}'", ORDERS_TOPIC, MARKET_DATA_TOPIC);
        // Let it run indefinitely
        Runtime.getRuntime().addShutdownHook(new Thread(executor::shutdown));
    }

    private static Runnable createProducer(String topic, ThrowingSupplier<String> recordSupplier, int sleepMs) {
        return () -> {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                //noinspection InfiniteLoopStatement
                while (true) {
                    String recordValue = recordSupplier.get();
                    log.info("Producing to topic '{}': {}", topic, recordValue);
                    producer.send(new ProducerRecord<>(topic, recordValue));
                    Thread.sleep(random.nextInt(sleepMs) + (sleepMs / 2));
                }
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.error("Producer for topic {} failed", topic, e);
            }
        };
    }

    @FunctionalInterface
    interface ThrowingSupplier<T> {
        T get() throws Exception;
    }


}
