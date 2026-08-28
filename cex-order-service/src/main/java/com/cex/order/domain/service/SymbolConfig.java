package com.cex.order.domain.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 交易对配置(领域模型)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SymbolConfig {

    public enum SymbolStatus {
        ACTIVE, PAUSED
    }

    private String symbol;
    private String baseCurrency;
    private String quoteCurrency;
    private int priceScale;
    private int quantityScale;
    private BigDecimal minQuantity;
    private BigDecimal minAmount;
    private SymbolStatus status;

    public boolean isTradable() {
        return status == SymbolStatus.ACTIVE;
    }
}
