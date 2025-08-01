package com.xchen.nav.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xchen.nav.conf.KafkaConfig;
import com.xchen.nav.function.NavCalculator;
import com.xchen.nav.function.PositionUpdater;
import com.xchen.nav.model.MarketData;
import com.xchen.nav.model.TradeOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class FlinkIgniteNavJob {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- Source for Trade Orders ---
        KafkaSource<String> ordersSource = KafkaConfig.kafkaSource(KafkaConfig.ORDERS_TOPIC, "order-group");
        DataStream<TradeOrder> orderStream = env.fromSource(ordersSource, WatermarkStrategy.noWatermarks(), "Kafka Orders")
                .map(json -> objectMapper.readValue(json, TradeOrder.class));

        // --- Source for Market Data ---
        KafkaSource<String> marketDataSource = KafkaConfig.kafkaSource(KafkaConfig.MARKET_DATA_TOPIC, "market-data-group");
        DataStream<MarketData> marketDataStream = env.fromSource(marketDataSource, WatermarkStrategy.noWatermarks(), "Kafka Market Data")
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

}