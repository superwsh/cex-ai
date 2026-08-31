package com.cex.matching.domain.service;

import com.cex.matching.domain.enums.OrderEventType;
import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.enums.OrderStatus;
import com.cex.matching.domain.enums.OrderType;
import com.cex.matching.domain.enums.TimeInForce;
import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.model.MatchResult;
import com.cex.matching.domain.model.OrderBook;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryMatchingEngineTest {

    private static final String SYMBOL = "BTC_USDT";

    @Test
    void process_usesBestMakerPricesAndCreatesTradesAcrossPriceLevels() {
        OrderBook book = new OrderBook(SYMBOL);
        book.addOrder(limit(1L, OrderSide.SELL, "100", "0.5", TimeInForce.GTC));
        book.addOrder(limit(2L, OrderSide.SELL, "101", "0.3", TimeInForce.GTC));
        InMemoryMatchingEngine engine = newEngine(book);

        MatchResult result = engine.process(limit(3L, OrderSide.BUY, "101", "0.7", TimeInForce.GTC));

        assertThat(result.getFinalStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(result.getRemainingQuantity()).isEqualByComparingTo("0");
        assertThat(result.getTrades()).hasSize(2);
        assertThat(result.getTrades())
                .extracting(trade -> trade.getPrice().toPlainString())
                .containsExactly("100", "101");
        assertThat(result.getTrades())
                .extracting(trade -> trade.getQuantity().toPlainString())
                .containsExactly("0.5", "0.2");
        assertThat(book.containsOrder(1L)).isFalse();
        assertThat(book.getOrder(2L).orElseThrow().getRemainingQuantity())
                .isEqualByComparingTo("0.1");
        assertThat(result.getEvents()).extracting(event -> event.getType())
                .contains(OrderEventType.ORDER_ACCEPTED, OrderEventType.TRADE_CREATED, OrderEventType.ORDER_FILLED);
    }

    @Test
    void process_preservesMakerFifoAtTheSamePrice() {
        OrderBook book = new OrderBook(SYMBOL);
        book.addOrder(limit(1L, OrderSide.SELL, "100", "0.3", TimeInForce.GTC));
        book.addOrder(limit(2L, OrderSide.SELL, "100", "0.4", TimeInForce.GTC));
        InMemoryMatchingEngine engine = newEngine(book);

        MatchResult result = engine.process(limit(3L, OrderSide.BUY, "100", "0.5", TimeInForce.GTC));

        assertThat(result.getTrades())
                .extracting(trade -> trade.getMakerOrderId())
                .containsExactly(1L, 2L);
        assertThat(book.containsOrder(1L)).isFalse();
        assertThat(book.getOrder(2L).orElseThrow().getRemainingQuantity())
                .isEqualByComparingTo("0.2");
    }

    @Test
    void process_keepsPartiallyFilledGtcOrderAsTheNewRestingOrder() {
        OrderBook book = new OrderBook(SYMBOL);
        book.addOrder(limit(1L, OrderSide.SELL, "100", "0.3", TimeInForce.GTC));
        InMemoryMatchingEngine engine = newEngine(book);
        MatchOrder taker = limit(2L, OrderSide.BUY, "100", "1", TimeInForce.GTC);

        MatchResult result = engine.process(taker);

        assertThat(result.getFinalStatus()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
        assertThat(result.getRemainingQuantity()).isEqualByComparingTo("0.7");
        assertThat(book.getBestBid().orElseThrow().getFirstOrder()).isSameAs(taker);
    }

    @Test
    void process_cancelsIocRemainderWithoutRestingIt() {
        OrderBook book = new OrderBook(SYMBOL);
        book.addOrder(limit(1L, OrderSide.SELL, "100", "0.4", TimeInForce.GTC));
        InMemoryMatchingEngine engine = newEngine(book);

        MatchResult result = engine.process(limit(2L, OrderSide.BUY, "100", "1", TimeInForce.IOC));

        assertThat(result.getFinalStatus()).isEqualTo(OrderStatus.CANCELED);
        assertThat(result.getRemainingQuantity()).isEqualByComparingTo("0.6");
        assertThat(book.getBestBid()).isEmpty();
        assertThat(result.getEvents()).extracting(event -> event.getType())
                .contains(OrderEventType.ORDER_PARTIALLY_FILLED, OrderEventType.ORDER_CANCELED);
        assertThat(engine.cancel(2L, 99L)).isSameAs(result);
    }

    @Test
    void process_rejectsFokExecutionWhenLiquidityIsInsufficientWithoutMutatingTheBook() {
        OrderBook book = new OrderBook(SYMBOL);
        MatchOrder maker = limit(1L, OrderSide.SELL, "100", "0.8", TimeInForce.GTC);
        book.addOrder(maker);
        InMemoryMatchingEngine engine = newEngine(book);

        MatchResult result = engine.process(limit(2L, OrderSide.BUY, "100", "1", TimeInForce.FOK));

        assertThat(result.getFinalStatus()).isEqualTo(OrderStatus.CANCELED);
        assertThat(result.getTrades()).isEmpty();
        assertThat(maker.getRemainingQuantity()).isEqualByComparingTo("0.8");
        assertThat(book.containsOrder(1L)).isTrue();
        assertThat(book.containsOrder(2L)).isFalse();
    }

    @Test
    void process_matchesMarketOrderAtBestPricesAndCancelsItsRemainder() {
        OrderBook book = new OrderBook(SYMBOL);
        book.addOrder(limit(1L, OrderSide.SELL, "100", "0.5", TimeInForce.GTC));
        book.addOrder(limit(2L, OrderSide.SELL, "101", "0.2", TimeInForce.GTC));
        InMemoryMatchingEngine engine = newEngine(book);

        MatchResult result = engine.process(marketBuy(3L, "1000"));

        assertThat(result.getFinalStatus()).isEqualTo(OrderStatus.CANCELED);
        assertThat(result.getRemainingQuantity()).isEqualByComparingTo("0");
        assertThat(result.getRemainingQuoteAmount()).isEqualByComparingTo("929.8");
        assertThat(result.getTrades()).extracting(trade -> trade.getPrice().toPlainString())
                .containsExactly("100", "101");
        assertThat(book.getBestAsk()).isEmpty();
        assertThat(book.getBestBid()).isEmpty();
    }

    @Test
    void cancel_isIdempotentAndRejectsFilledOrder() {
        OrderBook book = new OrderBook(SYMBOL);
        InMemoryMatchingEngine engine = newEngine(book);
        MatchOrder resting = limit(1L, OrderSide.BUY, "100", "1", TimeInForce.GTC);
        engine.process(resting);

        MatchResult firstCancel = engine.cancel(1L, 10L, Instant.parse("2026-01-01T00:00:00Z"));
        MatchResult repeatedCancel = engine.cancel(1L, 11L);
        MatchResult filledOrder = matchAndCancelFilledOrder(engine);

        assertThat(firstCancel.getFinalStatus()).isEqualTo(OrderStatus.CANCELED);
        assertThat(repeatedCancel).isSameAs(firstCancel);
        assertThat(filledOrder.getFinalStatus()).isEqualTo(OrderStatus.REJECTED);
    }

    private MatchResult matchAndCancelFilledOrder(InMemoryMatchingEngine engine) {
        engine.process(limit(2L, OrderSide.BUY, "100", "1", TimeInForce.GTC));
        engine.process(limit(3L, OrderSide.SELL, "100", "1", TimeInForce.GTC));
        return engine.cancel(3L, 12L);
    }

    private InMemoryMatchingEngine newEngine(OrderBook book) {
        AtomicLong sequence = new AtomicLong(10_000L);
        return new InMemoryMatchingEngine(book, sequence::incrementAndGet);
    }

    private MatchOrder limit(long orderId, OrderSide side, String price, String quantity, TimeInForce timeInForce) {
        return MatchOrder.builder()
                .orderId(orderId).userId(7L).symbol(SYMBOL).side(side)
                .type(OrderType.LIMIT).price(new BigDecimal(price))
                .quantity(new BigDecimal(quantity)).timeInForce(timeInForce)
                .createdAt(Instant.EPOCH).sequence(orderId).build();
    }

    private MatchOrder marketBuy(long orderId, String quoteAmount) {
        return MatchOrder.builder()
                .orderId(orderId).userId(7L).symbol(SYMBOL).side(OrderSide.BUY)
                .type(OrderType.MARKET).quantity(BigDecimal.ZERO).quoteAmount(new BigDecimal(quoteAmount))
                .timeInForce(TimeInForce.IOC).createdAt(Instant.EPOCH)
                .sequence(orderId).build();
    }
}
