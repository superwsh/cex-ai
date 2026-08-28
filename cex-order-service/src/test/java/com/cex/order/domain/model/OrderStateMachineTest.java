package com.cex.order.domain.model;

import com.cex.order.common.OrderStatusInvalidException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderStateMachineTest {

    private Order newOrder() {
        return Order.builder()
                .orderId(1L).userId(100L).clientOrderId("c1").symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .status(OrderStatus.PENDING_MATCH)
                .build();
    }

    /** 市价买单:quantity 恒为 ZERO,成交判断走金额维度(quoteAmount) */
    private Order newMarketBuyOrder() {
        return Order.builder()
                .orderId(2L).userId(100L).clientOrderId("c2").symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.MARKET)
                .quantity(BigDecimal.ZERO)
                .quoteAmount(new BigDecimal("5000"))
                .status(OrderStatus.PENDING_MATCH)
                .build();
    }

    @Test
    void pendingMatch_canCancel() {
        Order order = newOrder();
        order.cancel();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELED);
    }

    @Test
    void pendingMatch_canPartiallyFill() {
        Order order = newOrder();
        order.markPartiallyFilled(new BigDecimal("0.04"), new BigDecimal("4000"));
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
        assertThat(order.getFilledQuantity()).isEqualByComparingTo("0.04");
    }

    @Test
    void partialFill_canFill() {
        Order order = newOrder();
        order.markPartiallyFilled(new BigDecimal("0.04"), new BigDecimal("4000"));
        order.markPartiallyFilled(new BigDecimal("0.06"), new BigDecimal("6000"));
        assertThat(order.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(order.getFilledQuantity()).isEqualByComparingTo("0.1");
        assertThat(order.getFilledAmount()).isEqualByComparingTo("10000");
    }

    @Test
    void filled_cannotCancel() {
        Order order = newOrder();
        order.markPartiallyFilled(new BigDecimal("0.1"), new BigDecimal("10000"));
        assertThatThrownBy(order::cancel)
                .isInstanceOf(OrderStatusInvalidException.class)
                .hasMessageContaining("FILLED");
    }

    @Test
    void filled_cannotFillAgain() {
        Order order = newOrder();
        order.markPartiallyFilled(new BigDecimal("0.1"), new BigDecimal("10000"));
        assertThatThrownBy(() -> order.markPartiallyFilled(new BigDecimal("0.01"), new BigDecimal("1000")))
                .isInstanceOf(OrderStatusInvalidException.class);
    }

    @Test
    void canceled_cannotFill() {
        Order order = newOrder();
        order.cancel();
        assertThatThrownBy(() -> order.markPartiallyFilled(new BigDecimal("0.01"), new BigDecimal("1000")))
                .isInstanceOf(OrderStatusInvalidException.class);
    }

    @Test
    void reject_works() {
        Order order = newOrder();
        order.reject();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
    }

    @Test
    void markPartiallyFilled_withExceededQuantity_throws() {
        Order order = newOrder();
        assertThatThrownBy(() -> order.markPartiallyFilled(new BigDecimal("0.2"), new BigDecimal("20000")))
                .isInstanceOf(OrderStatusInvalidException.class)
                .hasMessageContaining("超过委托数量");
    }

    @Test
    void marketBuy_partialFill_tracksAmount() {
        Order order = newMarketBuyOrder();
        order.markPartiallyFilled(new BigDecimal("0.02"), new BigDecimal("2000"));
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
        assertThat(order.getFilledQuantity()).isEqualByComparingTo("0.02");
        assertThat(order.getFilledAmount()).isEqualByComparingTo("2000");
    }

    @Test
    void marketBuy_fullFill_whenAmountReachesQuote() {
        Order order = newMarketBuyOrder();
        order.markPartiallyFilled(new BigDecimal("0.02"), new BigDecimal("2000"));
        order.markPartiallyFilled(new BigDecimal("0.03"), new BigDecimal("3000"));
        assertThat(order.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(order.getFilledQuantity()).isEqualByComparingTo("0.05");
        assertThat(order.getFilledAmount()).isEqualByComparingTo("5000");
    }

    @Test
    void marketBuy_fillExceedingQuote_throws() {
        Order order = newMarketBuyOrder();
        assertThatThrownBy(() -> order.markPartiallyFilled(new BigDecimal("0.1"), new BigDecimal("6000")))
                .isInstanceOf(OrderStatusInvalidException.class)
                .hasMessageContaining("成交金额超过冻结金额");
    }
}
