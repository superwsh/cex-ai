package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.enums.OrderStatus;
import com.cex.matching.domain.enums.OrderType;
import com.cex.matching.domain.enums.TimeInForce;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class MatchOrderTest {

    @Test
    void shouldChangeStatusWhenOrderIsPartiallyAndFullyFilled() {
        MatchOrder order = limitOrder();

        order.decreaseRemainingQuantity(new BigDecimal("0.4"));
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
        assertThat(order.getRemainingQuantity()).isEqualByComparingTo("0.6");

        order.decreaseRemainingQuantity(new BigDecimal("0.6"));
        assertThat(order.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(order.getRemainingQuantity()).isEqualByComparingTo("0");
    }

    @Test
    void shouldRejectCancelWhenOrderHasBeenFilled() {
        MatchOrder order = limitOrder();
        order.decreaseRemainingQuantity(BigDecimal.ONE);

        assertThatIllegalStateException().isThrownBy(order::cancel);
    }

    @Test
    void shouldRejectPriceWhenMarketOrderHasPrice() {
        assertThatIllegalArgumentException().isThrownBy(() -> MatchOrder.builder()
                .orderId(1L).userId(2L).symbol("BTC_USDT").side(OrderSide.BUY)
                .type(OrderType.MARKET).price(BigDecimal.ONE).quantity(BigDecimal.ONE)
                .timeInForce(TimeInForce.IOC).createdAt(Instant.EPOCH).sequence(1L).build());
    }

    @Test
    void shouldTrackQuoteBudgetWhenMarketBuyOrderIsFilled() {
        MatchOrder order = MatchOrder.builder()
                .orderId(2L).userId(2L).symbol("BTC_USDT").side(OrderSide.BUY)
                .type(OrderType.MARKET).quantity(BigDecimal.ZERO).quoteAmount(new BigDecimal("100"))
                .timeInForce(TimeInForce.IOC).createdAt(Instant.EPOCH).sequence(2L).build();

        order.applyFill(new BigDecimal("0.4"), new BigDecimal("40"));

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
        assertThat(order.getFilledQuantity()).isEqualByComparingTo("0.4");
        assertThat(order.getRemainingQuoteAmount()).isEqualByComparingTo("60");
    }

    private MatchOrder limitOrder() {
        return MatchOrder.builder()
                .orderId(1L).userId(2L).symbol("BTC_USDT").side(OrderSide.BUY)
                .type(OrderType.LIMIT).price(new BigDecimal("100"))
                .quantity(BigDecimal.ONE).timeInForce(TimeInForce.GTC)
                .createdAt(Instant.EPOCH).sequence(1L).build();
    }
}
