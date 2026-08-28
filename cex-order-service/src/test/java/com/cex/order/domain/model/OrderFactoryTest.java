package com.cex.order.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class OrderFactoryTest {

    private final OrderFactory factory = new OrderFactory();

    @Test
    void createPendingMatchOrder_setsInitialState() {
        Order order = factory.createPendingMatchOrder(
                1L, 100L, "c1", "BTC_USDT",
                OrderSide.BUY, OrderType.LIMIT,
                new BigDecimal("100000"), new BigDecimal("0.1"), null,
                TimeInForce.GTC);

        assertThat(order.getOrderId()).isEqualTo(1L);
        assertThat(order.getId()).isEqualTo(1L);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING_MATCH);
        assertThat(order.getFilledQuantity()).isEqualByComparingTo("0");
        assertThat(order.getVersion()).isEqualTo(0L);
        assertThat(order.getCreatedAt()).isNotNull();
    }

    @Test
    void createMarketBuyOrder_keepsQuoteAmount() {
        Order order = factory.createPendingMatchOrder(
                2L, 100L, "c2", "BTC_USDT",
                OrderSide.BUY, OrderType.MARKET,
                null, BigDecimal.ZERO, new BigDecimal("1000"),
                TimeInForce.GTC);
        assertThat(order.getQuoteAmount()).isEqualByComparingTo("1000");
        assertThat(order.getPrice()).isNull();
    }
}
