package com.xchen.nav.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TradeOrder {
    private String orderId;
    private String fundId;
    private String stockSymbol;
    private int quantity; // Positive for buy, negative for sell
    private long timestamp;
}