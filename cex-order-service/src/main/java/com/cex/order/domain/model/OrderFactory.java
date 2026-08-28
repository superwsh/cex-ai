package com.cex.order.domain.model;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 订单工厂:构造初始状态为 PENDING_MATCH 的订单(创建即提交待撮合)
 */
@Component
public class OrderFactory {

    public Order createPendingMatchOrder(Long orderId, Long userId, String clientOrderId,
                                         String symbol, OrderSide side, OrderType type,
                                         BigDecimal price, BigDecimal quantity, BigDecimal quoteAmount,
                                         TimeInForce timeInForce) {
        LocalDateTime now = LocalDateTime.now();
        return Order.builder()
                .id(orderId)
                .orderId(orderId)
                .userId(userId)
                .clientOrderId(clientOrderId)
                .symbol(symbol)
                .side(side)
                .type(type)
                .price(price)
                .quantity(quantity)
                .quoteAmount(quoteAmount)
                .filledQuantity(BigDecimal.ZERO)
                .filledAmount(BigDecimal.ZERO)
                .status(OrderStatus.PENDING_MATCH)
                .timeInForce(timeInForce)
                .createdAt(now)
                .updatedAt(now)
                .version(0L)
                .build();
    }
}
