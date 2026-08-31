package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderSide;
import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/** 一次实际成交的不可变记录，成交价格始终使用挂单方价格。 */
public final class Trade {

    private final String tradeId;
    private final String symbol;
    private final long makerOrderId;
    private final long takerOrderId;
    private final OrderSide makerSide;
    private final BigDecimal price;
    private final BigDecimal quantity;
    private final BigDecimal quoteAmount;
    private final Instant timestamp;
    private final long sequence;

    /**
     * 创建成交记录并计算成交额；由 Lombok 生成的 Builder 调用。
     *
     * @param tradeId 成交编号
     * @param symbol 交易对
     * @param makerOrderId 挂单编号
     * @param takerOrderId 吃单编号
     * @param makerSide 挂单方向
     * @param price 成交价格
     * @param quantity 成交数量
     * @param timestamp 成交时间
     * @param sequence 撮合序号
     */
    @Builder
    private Trade(String tradeId, String symbol, long makerOrderId, long takerOrderId, OrderSide makerSide,
                  BigDecimal price, BigDecimal quantity, Instant timestamp, long sequence) {
        validate(tradeId, symbol, makerOrderId, takerOrderId, makerSide, price, quantity, timestamp, sequence);
        this.tradeId = tradeId;
        this.symbol = symbol;
        this.makerOrderId = makerOrderId;
        this.takerOrderId = takerOrderId;
        this.makerSide = makerSide;
        this.price = price;
        this.quantity = quantity;
        this.quoteAmount = price.multiply(quantity);
        this.timestamp = timestamp;
        this.sequence = sequence;
    }

    public String getTradeId() {
        return tradeId;
    }

    public String getSymbol() {
        return symbol;
    }

    public long getMakerOrderId() {
        return makerOrderId;
    }

    public long getTakerOrderId() {
        return takerOrderId;
    }

    /**
     * 获取挂单方的买卖方向。
     *
     * @return 挂单方买卖方向
     */
    public OrderSide getMakerSide() {
        return makerSide;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public BigDecimal getQuantity() {
        return quantity;
    }

    public BigDecimal getQuoteAmount() {
        return quoteAmount;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public long getSequence() {
        return sequence;
    }

    private static void validate(String tradeId, String symbol, long makerOrderId, long takerOrderId,
                                 OrderSide makerSide, BigDecimal price, BigDecimal quantity,
                                 Instant timestamp, long sequence) {
        if (tradeId == null || tradeId.isBlank() || makerOrderId <= 0
                || takerOrderId <= 0 || sequence < 0) {
            throw new IllegalArgumentException("成交标识和序号必须有效");
        }
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        requirePositive(price, "成交价格");
        requirePositive(quantity, "成交数量");
        Objects.requireNonNull(makerSide, "挂单方买卖方向不能为空");
        Objects.requireNonNull(timestamp, "成交时间不能为空");
    }

    private static void requirePositive(BigDecimal value, String fieldName) {
        if (value == null || value.signum() <= 0) {
            throw new IllegalArgumentException(fieldName + "必须大于零");
        }
    }

}
