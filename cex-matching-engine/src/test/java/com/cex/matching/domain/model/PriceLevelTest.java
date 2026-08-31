package com.cex.matching.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class PriceLevelTest {

    @Test
    void keepsSamePriceOrdersInArrivalOrderAndSumsRemainingQuantity() {
        PriceLevel level = new PriceLevel(new BigDecimal("100.00"));
        MatchOrder first = order(1L, "1.0");
        MatchOrder second = order(2L, "2.0");

        level.addOrder(first);
        level.addOrder(second);

        assertThat(level.peekFirst()).isSameAs(first);
        assertThat(level.totalRemainingQuantity()).isEqualByComparingTo("3.0");
        assertThat(level.removeOrder(1L)).isTrue();
        assertThat(level.peekFirst()).isSameAs(second);
    }

    private MatchOrder order(long orderId, String quantity) {
        BigDecimal value = new BigDecimal(quantity);
        return new MatchOrder(
                orderId, 10L, "BTC_USDT", MatchOrder.Side.BUY,
                new BigDecimal("100.00"), value, value, orderId);
    }
}
