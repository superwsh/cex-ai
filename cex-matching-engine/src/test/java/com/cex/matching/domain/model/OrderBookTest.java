package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderSide;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class OrderBookTest {

    private static final String SYMBOL = "BTC_USDT";

    @Test
    void addOrder_ordersBidsByDescendingPrice() {
        OrderBook book = new OrderBook(SYMBOL);
        book.addOrder(limit(1L, OrderSide.BUY, "100.00", "1"));
        book.addOrder(limit(2L, OrderSide.BUY, "101.00", "1"));

        assertThat(book.getBestBid()).isPresent();
        assertThat(book.getBestBid().orElseThrow().getPrice()).isEqualByComparingTo("101.00");
        assertThat(book.getBidDepth().keySet()).containsExactly(
                new BigDecimal("101.00"), new BigDecimal("100.00"));
    }

    @Test
    void addOrder_ordersAsksByAscendingPrice() {
        OrderBook book = new OrderBook(SYMBOL);
        book.addOrder(limit(1L, OrderSide.SELL, "101.00", "1"));
        book.addOrder(limit(2L, OrderSide.SELL, "100.00", "1"));

        assertThat(book.getBestAsk()).isPresent();
        assertThat(book.getBestAsk().orElseThrow().getPrice()).isEqualByComparingTo("100.00");
        assertThat(book.getAskDepth().keySet()).containsExactly(
                new BigDecimal("100.00"), new BigDecimal("101.00"));
    }

    @Test
    void addOrder_keepsTimePriorityWithinTheSamePriceLevel() {
        OrderBook book = new OrderBook(SYMBOL);
        MatchOrder first = limit(1L, OrderSide.SELL, "100", "0.2");
        MatchOrder second = limit(2L, OrderSide.SELL, "100", "0.3");
        MatchOrder third = limit(3L, OrderSide.SELL, "100", "0.5");

        book.addOrder(first);
        book.addOrder(second);
        book.addOrder(third);

        assertThat(book.getBestAsk().orElseThrow().getOrders())
                .extracting(MatchOrder::getOrderId)
                .containsExactly(1L, 2L, 3L);
        assertThat(book.getAskDepth()).containsEntry(new BigDecimal("100"), new BigDecimal("1.0"));
    }

    @Test
    void removeOrder_usesIndexAndRemovesAnEmptyPriceLevel() {
        OrderBook book = new OrderBook(SYMBOL);
        MatchOrder order = limit(1L, OrderSide.BUY, "100", "1");
        book.addOrder(order);

        assertThat(book.containsOrder(1L)).isTrue();
        assertThat(book.getOrder(1L)).containsSame(order);
        assertThat(book.removeOrder(1L)).containsSame(order);

        assertThat(book.containsOrder(1L)).isFalse();
        assertThat(book.getBestBid()).isEmpty();
        assertThat(book.getBidDepth()).isEmpty();
        assertThat(book.getActiveOrderCount()).isZero();
    }

    @Test
    void removeOrder_isIdempotentForAnUnknownOrder() {
        OrderBook book = new OrderBook(SYMBOL);

        assertThat(book.removeOrder(999L)).isEmpty();
    }

    @Test
    void addOrder_rejectsDuplicateOrderIdAndAnotherSymbol() {
        OrderBook book = new OrderBook(SYMBOL);
        book.addOrder(limit(1L, OrderSide.BUY, "100", "1"));

        assertThatIllegalArgumentException().isThrownBy(
                () -> book.addOrder(limit(1L, OrderSide.SELL, "101", "1")));
        assertThatIllegalArgumentException().isThrownBy(
                () -> book.addOrder(MatchOrder.builder()
                        .orderId(2L).userId(7L).symbol("ETH_USDT").side(OrderSide.BUY)
                        .type(com.cex.matching.domain.enums.OrderType.LIMIT)
                        .price(new BigDecimal("100")).quantity(BigDecimal.ONE)
                        .timeInForce(com.cex.matching.domain.enums.TimeInForce.GTC)
                        .createdAt(Instant.EPOCH).sequence(2L).build()));
    }

    private MatchOrder limit(long orderId, OrderSide side, String price, String quantity) {
        return MatchOrder.builder()
                .orderId(orderId).userId(7L).symbol(SYMBOL).side(side)
                .type(com.cex.matching.domain.enums.OrderType.LIMIT)
                .price(new BigDecimal(price)).quantity(new BigDecimal(quantity))
                .timeInForce(com.cex.matching.domain.enums.TimeInForce.GTC)
                .createdAt(Instant.EPOCH).sequence(orderId).build();
    }
}
