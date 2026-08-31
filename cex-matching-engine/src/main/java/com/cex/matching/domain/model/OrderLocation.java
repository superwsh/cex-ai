package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderSide;

import java.math.BigDecimal;
import java.util.Objects;

/** 用于快速定位挂单的索引项，避免扫描整个订单簿。 */
public final class OrderLocation {

    private final String symbol;
    private final OrderSide side;
    private final BigDecimal price;
    private final PriceLevel priceLevel;
    private final MatchOrder order;

    public OrderLocation(String symbol, OrderSide side, BigDecimal price,
                         PriceLevel priceLevel, MatchOrder order) {
        this.symbol = Objects.requireNonNull(symbol, "symbol must not be null");
        this.side = Objects.requireNonNull(side, "side must not be null");
        this.price = Objects.requireNonNull(price, "price must not be null");
        this.priceLevel = Objects.requireNonNull(priceLevel, "priceLevel must not be null");
        this.order = Objects.requireNonNull(order, "order must not be null");
    }

    public String getSymbol() {
        return symbol;
    }

    public OrderSide getSide() {
        return side;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public PriceLevel getPriceLevel() {
        return priceLevel;
    }

    public MatchOrder getOrder() {
        return order;
    }
}
