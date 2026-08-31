package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderEventType;
import lombok.Builder;

import java.time.Instant;
import java.math.BigDecimal;
import java.util.Objects;

/** 撮合领域生成的不可变事件，可作为下游幂等键的来源。 */
public final class OrderEvent {

    private final String eventId;
    private final long sequence;
    private final long orderId;
    private final String symbol;
    private final Instant timestamp;
    private final OrderEventType type;
    private final String tradeId;
    private final BigDecimal filledQuantity;
    private final BigDecimal remainingQuantity;

    /**
     * 创建不可变的订单结果事件；由 Lombok 生成的 Builder 调用。
     *
     * @param eventId 事件编号
     * @param sequence 撮合序号
     * @param orderId 订单编号
     * @param symbol 交易对
     * @param timestamp 事件时间
     * @param type 事件类型
     * @param tradeId 成交编号
     * @param filledQuantity 累计成交数量
     * @param remainingQuantity 剩余数量
     */
    @Builder
    private OrderEvent(String eventId, long sequence, long orderId, String symbol, Instant timestamp,
                       OrderEventType type, String tradeId, BigDecimal filledQuantity,
                       BigDecimal remainingQuantity) {
        validate(eventId, sequence, orderId, symbol, timestamp, type);
        this.eventId = eventId;
        this.sequence = sequence;
        this.orderId = orderId;
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.type = type;
        this.tradeId = tradeId;
        this.filledQuantity = filledQuantity;
        this.remainingQuantity = remainingQuantity;
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

    public String getTradeId() {
        return tradeId;
    }
    public BigDecimal getFilledQuantity() { return filledQuantity; }
    public BigDecimal getRemainingQuantity() { return remainingQuantity; }

    private static void validate(String eventId, long sequence, long orderId, String symbol, Instant timestamp,
                                 OrderEventType type) {
        if (eventId == null || eventId.isBlank() || sequence < 0 || orderId <= 0) {
            throw new IllegalArgumentException("事件标识和序号必须有效");
        }
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        Objects.requireNonNull(timestamp, "事件时间不能为空");
        Objects.requireNonNull(type, "事件类型不能为空");
    }
}
