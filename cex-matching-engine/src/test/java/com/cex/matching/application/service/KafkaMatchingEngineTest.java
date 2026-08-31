package com.cex.matching.application.service;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.common.kafka.event.TradeEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaMatchingEngineTest {

    @Test
    void match_shouldPublishTradeEventWithStableIdempotencyKeyAndOrderSides() {
        KafkaMatchingEngine matchingEngine = new KafkaMatchingEngine(new MatchingEngineRegistry());
        List<TradeEvent> tradeEvents = new ArrayList<>();
        matchingEngine.match(limitEvent("sell-event", "1", OrderEvent.OrderSide.SELL), tradeEvents::add);

        matchingEngine.match(limitEvent("buy-event", "2", OrderEvent.OrderSide.BUY), tradeEvents::add);

        assertThat(tradeEvents).hasSize(1);
        TradeEvent tradeEvent = tradeEvents.get(0);
        assertThat(tradeEvent.getEventId()).startsWith("trade-");
        assertThat(tradeEvent.getBuyOrderId()).isEqualTo("2");
        assertThat(tradeEvent.getSellOrderId()).isEqualTo("1");
        assertThat(tradeEvent.getAmount()).isEqualByComparingTo("50");
    }

    @Test
    void match_shouldGenerateSameTradeIdWhenReplayingSameCommands() {
        List<TradeEvent> first = matchPair(new KafkaMatchingEngine(new MatchingEngineRegistry()));
        List<TradeEvent> second = matchPair(new KafkaMatchingEngine(new MatchingEngineRegistry()));

        assertThat(second.get(0).getTradeId()).isEqualTo(first.get(0).getTradeId());
    }

    @Test
    void match_shouldPublishOrderResultEvents() {
        KafkaMatchingEngine matchingEngine = new KafkaMatchingEngine(new MatchingEngineRegistry());
        List<com.cex.common.kafka.event.OrderResultEvent> results = new ArrayList<>();
        matchingEngine.match(limitEvent("sell-event", "1", OrderEvent.OrderSide.SELL), ignored -> { }, results::add);
        matchingEngine.match(limitEvent("buy-event", "2", OrderEvent.OrderSide.BUY), ignored -> { }, results::add);

        assertThat(results).extracting(com.cex.common.kafka.event.OrderResultEvent::getType)
                .contains(com.cex.common.kafka.event.OrderResultEvent.OrderResultType.ORDER_FILLED);
    }

    private List<TradeEvent> matchPair(KafkaMatchingEngine matchingEngine) {
        List<TradeEvent> events = new ArrayList<>();
        matchingEngine.match(limitEvent("sell-event", "1", OrderEvent.OrderSide.SELL), events::add);
        matchingEngine.match(limitEvent("buy-event", "2", OrderEvent.OrderSide.BUY), events::add);
        return events;
    }

    private OrderEvent limitEvent(String eventId, String orderId, OrderEvent.OrderSide side) {
        return OrderEvent.builder()
                .eventId(eventId)
                .orderId(orderId)
                .userId(7L)
                .symbol("BTC_USDT")
                .action(OrderEvent.Action.SUBMIT)
                .side(side)
                .type(OrderEvent.OrderType.LIMIT)
                .price(new BigDecimal("100"))
                .quantity(new BigDecimal("0.5"))
                .timeInForce(OrderEvent.TimeInForce.GTC)
                .timestamp(1L)
                .build();
    }
}
