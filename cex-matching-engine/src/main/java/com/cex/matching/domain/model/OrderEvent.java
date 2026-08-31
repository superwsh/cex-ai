package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderEventType;

import java.time.Instant;
import java.util.Objects;

/** 撮合领域生成的不可变事件，可作为下游幂等键的来源。 */
public final class OrderEvent {

    private final String eventId;
    private final long sequence;
    private final long orderId;
    private final String symbol;
    private final Instant timestamp;
    private final OrderEventType type;
    private final Long tradeId;

    private OrderEvent(Builder builder) {
        validateBuilder(builder);
        this.eventId = builder.eventId;
        this.sequence = builder.sequence;
        this.orderId = builder.orderId;
        this.symbol = builder.symbol;
        this.timestamp = builder.timestamp;
        this.type = builder.type;
        this.tradeId = builder.tradeId;
    }

    /** 创建事件构造器，避免多参数构造函数。 */
    public static Builder builder() {
        return new Builder();
    }

    public String getEventId() {
        return eventId;
    }

    public long getSequence() {
        return sequence;
    }

    public long getOrderId() {
        return orderId;
    }

    public String getSymbol() {
        return symbol;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public OrderEventType getType() {
        return type;
    }

    public Long getTradeId() {
        return tradeId;
    }

    private static void validateBuilder(Builder builder) {
        if (builder.eventId == null || builder.eventId.isBlank()
                || builder.sequence < 0 || builder.orderId <= 0) {
            throw new IllegalArgumentException("事件标识和序号必须有效");
        }
        if (builder.symbol == null || builder.symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        Objects.requireNonNull(builder.timestamp, "事件时间不能为空");
        Objects.requireNonNull(builder.type, "事件类型不能为空");
    }

    /** 用于构造领域事件的构造器。 */
    public static final class Builder {

        private String eventId;
        private long sequence;
        private long orderId;
        private String symbol;
        private Instant timestamp;
        private OrderEventType type;
        private Long tradeId;

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder sequence(long sequence) {
            this.sequence = sequence;
            return this;
        }

        public Builder orderId(long orderId) {
            this.orderId = orderId;
            return this;
        }

        public Builder symbol(String symbol) {
            this.symbol = symbol;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder type(OrderEventType type) {
            this.type = type;
            return this;
        }

        public Builder tradeId(Long tradeId) {
            this.tradeId = tradeId;
            return this;
        }

        /** 校验并创建事件。 */
        public OrderEvent build() {
            return new OrderEvent(this);
        }
    }
}
