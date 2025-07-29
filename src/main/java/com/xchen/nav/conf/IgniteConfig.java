package com.xchen.nav.conf;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.client.IgniteClient;

@Slf4j
public class IgniteConfig {

    public static final String IGNITE_HOST = "localhost";
    public static final int IGNITE_PORT = 10800;
    public static final String POSITIONS_TABLE = "positions";
    public static final String MARKET_TABLE = "market_prices";

    public static void initTable() {
        try (IgniteClient client = IgniteClient.builder().addresses(IGNITE_HOST + ":" + IGNITE_PORT).build()) {
            log.info("--- Creating position table ---");
            client.catalog().createTable(
                    TableDefinition.builder(POSITIONS_TABLE)
                            .ifNotExists()
                            .columns(
                                    ColumnDefinition.column("fundId", ColumnType.VARCHAR),
                                    ColumnDefinition.column("stockSymbol", ColumnType.VARCHAR),
                                    ColumnDefinition.column("quantity", ColumnType.INT32)
                            )
                            .primaryKey("fundId", "stockSymbol")
                            .build()
            );
            log.info("--- Creating market_prices table ---");
            client.catalog().createTable(
                    TableDefinition.builder(MARKET_TABLE)
                            .ifNotExists()
                            .columns(
                                    ColumnDefinition.column("stockSymbol", ColumnType.VARCHAR),
                                    ColumnDefinition.column("price", ColumnType.DOUBLE),
                                    ColumnDefinition.column("lastUpdateTime", ColumnType.BIGINT)
                            )
                            .primaryKey("stockSymbol")
                            .build()
            );
        }

    }
}

