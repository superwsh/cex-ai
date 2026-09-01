package com.cex.market.domain.trade;

import com.cex.common.kafka.event.TradeEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** 24 小时分钟桶滚动统计测试。 */
class Ticker24hRollingWindowTest {

    private static final String SYMBOL = "BTC_USDT";
    private static final long NOW = 1_700_100_000_000L;

    @Test
    void shouldAggregateOpenHighLowCloseAndVolumesWithinRollingWindow() {
        Ticker24hRollingWindow window = new Ticker24hRollingWindow(SYMBOL);

        window.add(trade("T-1", "100", "2", NOW - 23 * 60 * 60 * 1_000L), NOW);
        window.add(trade("T-2", "120", "3", NOW - 30 * 60 * 1_000L), NOW);
        Ticker24h ticker = window.add(trade("T-3", "110", "4", NOW), NOW).orElseThrow();

        assertThat(ticker.openPrice()).isEqualByComparingTo("100");
        assertThat(ticker.highPrice()).isEqualByComparingTo("120");
        assertThat(ticker.lowPrice()).isEqualByComparingTo("100");
        assertThat(ticker.lastPrice()).isEqualByComparingTo("110");
        assertThat(ticker.priceChange()).isEqualByComparingTo("10");
        assertThat(ticker.priceChangePercent()).isEqualByComparingTo("10.00000000");
        assertThat(ticker.volume()).isEqualByComparingTo("9");
        assertThat(ticker.quoteVolume()).isEqualByComparingTo("1000");
        assertThat(ticker.tradeCount()).isEqualTo(3L);
    }

    @Test
    void shouldExcludeTradeOlderThanTwentyFourHours() {
        Ticker24hRollingWindow window = new Ticker24hRollingWindow(SYMBOL);

        assertThat(window.add(trade("T-old", "90", "1", NOW - 24 * 60 * 60 * 1_000L - 1L), NOW)).isEmpty();
        Ticker24h ticker = window.add(trade("T-new", "100", "1", NOW), NOW).orElseThrow();

        assertThat(ticker.openPrice()).isEqualByComparingTo("100");
        assertThat(ticker.tradeCount()).isEqualTo(1L);
    }

    @Test
    void shouldExpireTickerWhenNoTradeRemainsInTheRollingWindow() {
        Ticker24hRollingWindow window = new Ticker24hRollingWindow(SYMBOL);
        window.add(trade("T-1", "100", "1", NOW), NOW).orElseThrow();

        assertThat(window.current(NOW + 24 * 60 * 60 * 1_000L + 60_000L)).isEmpty();
    }

    private MarketTrade trade(String tradeId, String price, String quantity, long timestamp) {
        BigDecimal decimalPrice = new BigDecimal(price);
        BigDecimal decimalQuantity = new BigDecimal(quantity);
        return new MarketTrade(tradeId, SYMBOL, decimalPrice, decimalQuantity,
                decimalPrice.multiply(decimalQuantity), TradeEvent.TakerSide.BUY, timestamp);
    }
}
