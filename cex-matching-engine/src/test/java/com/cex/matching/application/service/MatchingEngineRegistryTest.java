package com.cex.matching.application.service;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.matching.domain.enums.OrderStatus;
import com.cex.matching.domain.model.MatchResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.cex.matching.domain.sequence.SequenceGapException;

class MatchingEngineRegistryTest {

    @Test
    void process_shouldRejectSequenceGapBeforeWalAndMatching() {
        MatchingEngineRegistry registry = new MatchingEngineRegistry();
        OrderEvent event = limitEvent("gap", "BTC_USDT", "1", OrderEvent.OrderSide.BUY, "100", "1");
        event.setSequence(2L);

        assertThatThrownBy(() -> registry.process(event))
                .isInstanceOf(SequenceGapException.class)
                .hasMessageContaining("expected=1");
        assertThat(registry.snapshot("BTC_USDT").orElseThrow().sequence()).isZero();
    }

    @Test
    void process_shouldMatchQuoteBudgetMarketBuyAndReturnCachedResultForDuplicateEvent() {
        MatchingEngineRegistry registry = new MatchingEngineRegistry();
        registry.process(limitEvent("sell-1", "BTC_USDT", "1", OrderEvent.OrderSide.SELL,
                "100", "0.5"));

        MatchResult result = registry.process(marketBuyEvent("buy-1", "BTC_USDT", "2", "50"))
                .orElseThrow();

        assertThat(result.getFinalStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(result.getTrades()).hasSize(1);
        assertThat(result.getTrades().get(0).getQuoteAmount()).isEqualByComparingTo("50");
        assertThat(registry.process(marketBuyEvent("buy-1", "BTC_USDT", "2", "50")))
                .containsSame(result);
    }

    @Test
    void process_shouldKeepDifferentSymbolsInIndependentOrderBooks() {
        MatchingEngineRegistry registry = new MatchingEngineRegistry();
        registry.process(limitEvent("btc-sell", "BTC_USDT", "1", OrderEvent.OrderSide.SELL,
                "100", "1"));
        registry.process(limitEvent("eth-buy", "ETH_USDT", "2", OrderEvent.OrderSide.BUY,
                "100", "1"));

        MatchResult btcResult = registry.process(limitEvent("btc-buy", "BTC_USDT", "3",
                OrderEvent.OrderSide.BUY, "100", "1")).orElseThrow();

        assertThat(btcResult.getTrades()).hasSize(1);
        assertThat(btcResult.getTrades().get(0).getSymbol()).isEqualTo("BTC_USDT");
    }

    @Test
    void process_shouldRecordCommandTradeAndActiveOrderMetrics() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        MatchingEngineRegistry registry = new MatchingEngineRegistry(new InMemoryMatchingCommandJournal(),
                new MatchingMetrics(meterRegistry));

        registry.process(limitEvent("buy-1", "BTC_USDT", "1", OrderEvent.OrderSide.BUY, "100", "1"));

        assertThat(meterRegistry.find("cex.matching.commands.total").counter()).isNotNull();
        assertThat(meterRegistry.find("cex.matching.active.orders").gauge()).isNotNull();
        assertThat(meterRegistry.find("cex.matching.active.orders").gauge().value()).isEqualTo(1.0D);
    }

    @Test
    void process_shouldKeepConcurrentDifferentSymbolShardsIndependent() {
        MatchingEngineRegistry registry = new MatchingEngineRegistry(new InMemoryMatchingCommandJournal());
        registry.process(limitEvent("btc-sell", "BTC_USDT", "1", OrderEvent.OrderSide.SELL, "100", "1"));
        registry.process(limitEvent("eth-sell", "ETH_USDT", "2", OrderEvent.OrderSide.SELL, "200", "1"));
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<MatchResult> btcResult = CompletableFuture.supplyAsync(() -> registry.process(
                    limitEvent("btc-buy", "BTC_USDT", "3", OrderEvent.OrderSide.BUY, "100", "1"))
                    .orElseThrow(), executorService);
            CompletableFuture<MatchResult> ethResult = CompletableFuture.supplyAsync(() -> registry.process(
                    limitEvent("eth-buy", "ETH_USDT", "4", OrderEvent.OrderSide.BUY, "200", "1"))
                    .orElseThrow(), executorService);

            assertThat(btcResult.join().getTrades()).singleElement()
                    .extracting(trade -> trade.getSymbol()).isEqualTo("BTC_USDT");
            assertThat(ethResult.join().getTrades()).singleElement()
                    .extracting(trade -> trade.getSymbol()).isEqualTo("ETH_USDT");
        } finally {
            executorService.shutdownNow();
        }
    }

    private OrderEvent limitEvent(String eventId, String symbol, String orderId, OrderEvent.OrderSide side,
                                  String price, String quantity) {
        return OrderEvent.builder()
                .eventId(eventId)
                .orderId(orderId)
                .userId(7L)
                .symbol(symbol)
                .action(OrderEvent.Action.SUBMIT)
                .side(side)
                .type(OrderEvent.OrderType.LIMIT)
                .price(new BigDecimal(price))
                .quantity(new BigDecimal(quantity))
                .timeInForce(OrderEvent.TimeInForce.GTC)
                .timestamp(1L)
                .build();
    }

    private OrderEvent marketBuyEvent(String eventId, String symbol, String orderId, String quoteAmount) {
        return OrderEvent.builder()
                .eventId(eventId)
                .orderId(orderId)
                .userId(7L)
                .symbol(symbol)
                .action(OrderEvent.Action.SUBMIT)
                .side(OrderEvent.OrderSide.BUY)
                .type(OrderEvent.OrderType.MARKET)
                .quantity(BigDecimal.ZERO)
                .quoteAmount(new BigDecimal(quoteAmount))
                .timeInForce(OrderEvent.TimeInForce.IOC)
                .timestamp(2L)
                .build();
    }
}
