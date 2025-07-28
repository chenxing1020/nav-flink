package com.xchen.nav.job;

import com.xchen.nav.DataGenerator;
import com.xchen.nav.function.NavCalculator;
import com.xchen.nav.function.PositionUpdater;
import com.xchen.nav.model.MarketData;
import com.xchen.nav.model.TradeOrder;
import com.xchen.nav.util.JacksonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class FlinkIgniteNavJob {

    private static final String IGNITE_HOST = "localhost";
    private static final int IGNITE_PORT = 10800;
    private static final String POSITIONS_TABLE = "positions";
    private static final String PRICES_TABLE = "market_prices";


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- Source for Trade Orders ---
        KafkaSource<String> ordersSource = createKafkaSource(DataGenerator.ORDERS_TOPIC, "order-group");
        DataStream<TradeOrder> orderStream = env.fromSource(ordersSource, new SimpleStringSchema(), "Kafka Orders")
                .map(json -> JacksonUtil.parseJson(json, TradeOrder.class));

        // --- Source for Market Data ---
        KafkaSource<String> marketDataSource = createKafkaSource(DataGenerator.MARKET_DATA_TOPIC, "market-data-group");
        DataStream<MarketData> marketDataStream = env.fromSource(marketDataSource, new SimpleStringSchema(), "Kafka Market Data")
                .map(json -> objectMapper.readValue(json, MarketData.class));

        // --- Processing Logic ---

        // 1. Orders stream: Update positions in Ignite. This stream terminates here.
        orderStream.map(new PositionUpdater()).name("Position Updater");

        // 2. Market data stream: Update prices in Ignite and then calculate NAV.
        DataStream<String> navStream = marketDataStream.map(new NavCalculator()).name("NAV Calculator");

        // 3. Print the results to the console
        navStream.print();

        env.execute("Flink Ignite NAV Calculation Job");
    }

    private static KafkaSource<String> createKafkaSource(String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(DataGenerator.KAFKA_BROKER)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
    }


}