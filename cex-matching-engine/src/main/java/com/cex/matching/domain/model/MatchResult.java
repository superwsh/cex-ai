package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderStatus;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

/** 订单处理结束后返回的结果。 */
public final class MatchResult {

    private final long orderId;
    private final OrderStatus finalStatus;
    private final BigDecimal remainingQuantity;
    private final BigDecimal remainingQuoteAmount;
    private final List<Trade> trades;
    private final List<OrderEvent> events;

    /**
     * 根据已处理订单创建不可变撮合结果。
     *
     * @param order 已完成本次命令处理的订单
     * @param trades 本次生成的成交记录
     * @param events 本次生成的领域事件
     */
    public MatchResult(MatchOrder order, List<Trade> trades, List<OrderEvent> events) {
        Objects.requireNonNull(order, "订单不能为空");
        this.orderId = order.getOrderId();
        this.finalStatus = order.getStatus();
        this.remainingQuantity = order.getRemainingQuantity();
        this.remainingQuoteAmount = order.getRemainingQuoteAmount();
        this.trades = List.copyOf(Objects.requireNonNull(trades, "trades must not be null"));
        this.events = List.copyOf(Objects.requireNonNull(events, "events must not be null"));
    }

    /**
     * 为无法定位订单的拒绝命令创建结果。
     *
     * @param orderId 命令关联的订单编号
     * @param finalStatus 命令处理后的订单状态
     * @param remainingQuantity 剩余基础资产数量
     * @param trades 本次生成的成交记录
     * @param events 本次生成的领域事件
     */
    public MatchResult(long orderId, OrderStatus finalStatus, BigDecimal remainingQuantity,
                       List<Trade> trades, List<OrderEvent> events) {
        this(orderId, finalStatus, remainingQuantity, null, trades, events);
    }

    /**
     * 根据持久化的命令结果重建不可变撮合结果。
     *
     * @param orderId 命令关联的订单编号
     * @param finalStatus 命令处理后的订单状态
     * @param remainingQuantity 剩余基础资产数量
     * @param remainingQuoteAmount 剩余计价资产预算；非市价买单为空
     * @param trades 本次生成的成交记录
     * @param events 本次生成的领域事件
     */
    public MatchResult(long orderId, OrderStatus finalStatus, BigDecimal remainingQuantity,
                       BigDecimal remainingQuoteAmount, List<Trade> trades, List<OrderEvent> events) {
        this.orderId = orderId;
        this.finalStatus = Objects.requireNonNull(finalStatus, "最终状态不能为空");
        this.remainingQuantity = Objects.requireNonNull(remainingQuantity, "剩余数量不能为空");
        this.remainingQuoteAmount = remainingQuoteAmount;
        this.trades = List.copyOf(Objects.requireNonNull(trades, "成交记录不能为空"));
        this.events = List.copyOf(Objects.requireNonNull(events, "领域事件不能为空"));
    }

    public long getOrderId() {
        return orderId;
    }

    public OrderStatus getFinalStatus() {
        return finalStatus;
    }

    public BigDecimal getRemainingQuantity() {
        return remainingQuantity;
    }

    /**
     * 获取市价买单剩余的计价资产预算；其他订单返回 null。
     *
     * @return 剩余计价资产预算
     */
    public BigDecimal getRemainingQuoteAmount() {
        return remainingQuoteAmount;
    }

    public List<Trade> getTrades() {
        return trades;
    }

    public List<OrderEvent> getEvents() {
        return events;
    }
}
