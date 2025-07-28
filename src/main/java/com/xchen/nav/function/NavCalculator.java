package com.xchen.nav.function;

import com.xchen.nav.conf.IgniteConfig;
import com.xchen.nav.model.MarketData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Updates the market price for a stock and then calculates the total NAV for FUND-A.
 */
@Slf4j
public class NavCalculator extends RichMapFunction<MarketData, String> {

    private transient IgniteClient igniteClient;
    private transient RecordView<Tuple> pricesView;
    private transient RecordView<Tuple> positionsView;

    @Override
    public void open(Configuration parameters) {
        igniteClient = IgniteClient.builder().addresses(IgniteConfig.IGNITE_HOST + ":" + IgniteConfig.IGNITE_PORT).build();
        pricesView = igniteClient.tables().table(IgniteConfig.PRICES_TABLE).recordView();
        positionsView = igniteClient.tables().table(IgniteConfig.POSITIONS_TABLE).recordView();
    }

    @Override
    public String map(MarketData data) {
        // 1. Update the latest price in Ignite
        Tuple priceRecord = Tuple.create()
                .set("stockSymbol", data.getStockSymbol())
                .set("price", data.getPrice())
                .set("lastUpdateTime", data.getTimestamp());
        pricesView.upsert(null, priceRecord);

        // 2. Calculate NAV for FUND-A
        BigDecimal totalNav = BigDecimal.ZERO;
        // In a real system, you'd iterate over all funds. Here we hardcode for the demo.
        positionsView.getAll()
        try (var cursor = positionsView.scan(null, null)) {
            while (cursor.hasNext()) {
                Tuple position = cursor.next();
                if ("FUND-A".equals(position.stringValue("fundId"))) {
                    String symbol = position.stringValue("stockSymbol");
                    int quantity = position.intValue("quantity");

                    // Get the latest price for this stock
                    Tuple priceKey = Tuple.create().set("stockSymbol", symbol);
                    Tuple latestPrice = pricesView.get(null, priceKey);

                    if (latestPrice != null) {
                        double price = latestPrice.doubleValue("price");
                        totalNav = totalNav.add(BigDecimal.valueOf(quantity).multiply(BigDecimal.valueOf(price)));
                    }
                }
            }
        }

        String result = String.format("FUND-A NAV as of %s: $%,.2f",
                java.time.Instant.now(), totalNav.setScale(2, RoundingMode.HALF_UP));
        log.info(result);
        return result;
    }

    @Override
    public void close() {
        if (igniteClient != null) igniteClient.close();
    }
}

