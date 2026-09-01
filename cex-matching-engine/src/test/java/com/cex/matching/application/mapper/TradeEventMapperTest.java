package com.cex.matching.application.mapper;

import com.cex.common.kafka.event.TradeEvent;
import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.model.Trade;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** 撮合成交到公共事件的行情契约映射测试。 */
class TradeEventMapperTest {

    private final TradeEventMapper mapper = new TradeEventMapper();

    @Test
    void shouldPopulateMarketFieldsForBuyTaker() {
        TradeEvent event = mapper.toTradeEvent(trade(OrderSide.SELL));

        assertThat(event.getEventVersion()).isEqualTo(1);
        assertThat(event.getQuoteQuantity()).isEqualByComparingTo("123.0000");
        assertThat(event.getAmount()).isEqualByComparingTo(event.getQuoteQuantity());
        assertThat(event.getTakerSide()).isEqualTo(TradeEvent.TakerSide.BUY);
        assertThat(event.getCreatedAt()).isEqualTo(1_700_000_000_000L);
    }

    @Test
    void shouldPopulateSellTakerWhenMakerIsBuyer() {
        TradeEvent event = mapper.toTradeEvent(trade(OrderSide.BUY));

        assertThat(event.getTakerSide()).isEqualTo(TradeEvent.TakerSide.SELL);
        assertThat(event.getBuyOrderId()).isEqualTo("11");
        assertThat(event.getSellOrderId()).isEqualTo("22");
    }

    private Trade trade(OrderSide makerSide) {
        return Trade.builder().tradeId("1").symbol("BTC_USDT").baseAsset("BTC").quoteAsset("USDT")
                .makerOrderId(11L).takerOrderId(22L).makerUserId(101L).takerUserId(202L).makerSide(makerSide)
                .price(new BigDecimal("123.0000")).quantity(BigDecimal.ONE)
                .timestamp(Instant.ofEpochMilli(1_700_000_000_000L)).sequence(88L).build();
    }
}
