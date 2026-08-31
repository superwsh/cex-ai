package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderSide;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/** 一次实际成交的不可变记录，成交价格始终使用挂单方价格。 */
public final class Trade {

    private final long tradeId;
    private final String symbol;
    private final long makerOrderId;
    private final long takerOrderId;
    private final OrderSide makerSide;
    private final BigDecimal price;
    private final BigDecimal quantity;
    private final BigDecimal quoteAmount;
    private final Instant timestamp;
    private final long sequence;

    private Trade(Builder builder) {
        validateBuilder(builder);
        this.tradeId = builder.tradeId;
        this.symbol = builder.symbol;
        this.makerOrderId = builder.makerOrderId;
        this.takerOrderId = builder.takerOrderId;
        this.makerSide = builder.makerSide;
        this.price = builder.price;
        this.quantity = builder.quantity;
        this.quoteAmount = builder.price.multiply(builder.quantity);
        this.timestamp = builder.timestamp;
        this.sequence = builder.sequence;
    }

    /** 创建成交构造器，避免多参数构造函数。 */
    public static Builder builder() {
        return new Builder();
    }

    public long getTradeId() {
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

    private static void validateBuilder(Builder builder) {
        if (builder.tradeId <= 0 || builder.makerOrderId <= 0
                || builder.takerOrderId <= 0 || builder.sequence < 0) {
            throw new IllegalArgumentException("成交标识和序号必须有效");
        }
        if (builder.symbol == null || builder.symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        requirePositive(builder.price, "成交价格");
        requirePositive(builder.quantity, "成交数量");
        Objects.requireNonNull(builder.makerSide, "挂单方买卖方向不能为空");
        Objects.requireNonNull(builder.timestamp, "成交时间不能为空");
    }

    private static void requirePositive(BigDecimal value, String fieldName) {
        if (value == null || value.signum() <= 0) {
            throw new IllegalArgumentException(fieldName + "必须大于零");
        }
    }

    /** 用于构造成交记录的构造器。 */
    public static final class Builder {

        private long tradeId;
        private String symbol;
        private long makerOrderId;
        private long takerOrderId;
        private OrderSide makerSide;
        private BigDecimal price;
        private BigDecimal quantity;
        private Instant timestamp;
        private long sequence;

        public Builder tradeId(long tradeId) {
            this.tradeId = tradeId;
            return this;
        }

        public Builder symbol(String symbol) {
            this.symbol = symbol;
            return this;
        }

        public Builder makerOrderId(long makerOrderId) {
            this.makerOrderId = makerOrderId;
            return this;
        }

        public Builder takerOrderId(long takerOrderId) {
            this.takerOrderId = takerOrderId;
            return this;
        }

        /**
         * 设置挂单方买卖方向。
         *
         * @param makerSide 挂单方买卖方向
         * @return 当前构造器
         */
        public Builder makerSide(OrderSide makerSide) {
            this.makerSide = makerSide;
            return this;
        }

        public Builder price(BigDecimal price) {
            this.price = price;
            return this;
        }

        public Builder quantity(BigDecimal quantity) {
            this.quantity = quantity;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder sequence(long sequence) {
            this.sequence = sequence;
            return this;
        }

        /** 校验并创建成交记录。 */
        public Trade build() {
            return new Trade(this);
        }
    }
}
