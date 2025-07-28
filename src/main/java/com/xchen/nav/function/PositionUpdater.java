package com.xchen.nav.function;

import com.xchen.nav.conf.IgniteConfig;
import com.xchen.nav.model.TradeOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;


/**
 * Updates the fund's position for a given stock based on a trade order.
 */
@Slf4j
public class PositionUpdater extends RichMapFunction<TradeOrder, Void> {
    private transient IgniteClient igniteClient;
    private transient KeyValueView<Tuple, Tuple> positionsView;

    @Override
    public void open(Configuration parameters) {
        igniteClient = IgniteClient.builder().addresses(IgniteConfig.IGNITE_HOST + ":" + IgniteConfig.IGNITE_PORT).build();
        positionsView = igniteClient.tables().table(IgniteConfig.POSITIONS_TABLE).keyValueView();

    }

    @Override
    public Void map(TradeOrder order) {
        log.info("Processing order: {} shares of {} for fund {}", order.getQuantity(), order.getStockSymbol(), order.getFundId());
        Tuple key = Tuple.create().set("fundId", order.getFundId()).set("stockSymbol", order.getStockSymbol());

        // Atomically update the position
        Tuple current = positionsView.get(null, key);
        int currentQuantity = 0;
        if (current != null && current.value("quantity") != null) {
            currentQuantity = current.value("quantity");
        }
        int newQuantity = currentQuantity + order.getQuantity();
        positionsView.put(null, key, Tuple.create().set("quantity", newQuantity));
        return null;
    }

    @Override
    public void close() {
        if (igniteClient != null) igniteClient.close();
    }
}