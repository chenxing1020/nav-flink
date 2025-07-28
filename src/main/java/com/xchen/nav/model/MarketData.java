package com.xchen.nav.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MarketData {
    private String stockSymbol;
    private double price;
    private long timestamp;
}
