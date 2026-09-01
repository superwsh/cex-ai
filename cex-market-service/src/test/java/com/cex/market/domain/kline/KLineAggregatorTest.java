package com.cex.market.domain.kline;

import com.cex.common.kafka.event.TradeEvent;
import com.cex.market.domain.trade.MarketTrade;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** KLine 开高低收、成交量和窗口切换测试。 */
class KLineAggregatorTest {

    private static final String SYMBOL = "BTC_USDT";

    @Test
    void shouldAggregateOpenHighLowCloseAndVolumesInSameWindow() {
        KLineAggregator aggregator = new KLineAggregator(SYMBOL);
        aggregator.apply(trade("T-1", "100", "2", 60_001L));
        KLineAggregationResult result = aggregator.apply(trade("T-2", "120", "3", 60_500L));
        KLine oneMinute = result.currentKlines().stream()
                .filter(kLine -> kLine.interval() == KLineInterval.ONE_MINUTE).findFirst().orElseThrow();

        assertThat(oneMinute.open()).isEqualByComparingTo("100");
        assertThat(oneMinute.high()).isEqualByComparingTo("120");
        assertThat(oneMinute.low()).isEqualByComparingTo("100");
        assertThat(oneMinute.close()).isEqualByComparingTo("120");
        assertThat(oneMinute.volume()).isEqualByComparingTo("5");
        assertThat(oneMinute.quoteVolume()).isEqualByComparingTo("560");
        assertThat(oneMinute.tradeCount()).isEqualTo(2L);
        assertThat(oneMinute.closed()).isFalse();
    }

    @Test
    void shouldClosePreviousKLineAndStartNextWindow() {
        KLineAggregator aggregator = new KLineAggregator(SYMBOL);
        aggregator.apply(trade("T-1", "100", "2", 60_001L));
        KLineAggregationResult result = aggregator.apply(trade("T-2", "110", "1", 120_001L));

        KLine closed = result.closedKlines().stream()
                .filter(kLine -> kLine.interval() == KLineInterval.ONE_MINUTE).findFirst().orElseThrow();
        KLine current = result.currentKlines().stream()
                .filter(kLine -> kLine.interval() == KLineInterval.ONE_MINUTE).findFirst().orElseThrow();
        assertThat(closed.openTime()).isEqualTo(60_000L);
        assertThat(closed.closeTime()).isEqualTo(119_999L);
        assertThat(closed.closed()).isTrue();
        assertThat(current.openTime()).isEqualTo(120_000L);
        assertThat(current.open()).isEqualByComparingTo("110");
        assertThat(current.tradeCount()).isEqualTo(1L);
    }

    private MarketTrade trade(String tradeId, String price, String quantity, long timestamp) {
        BigDecimal decimalPrice = new BigDecimal(price);
        BigDecimal decimalQuantity = new BigDecimal(quantity);
        return new MarketTrade(tradeId, SYMBOL, decimalPrice, decimalQuantity,
                decimalPrice.multiply(decimalQuantity), TradeEvent.TakerSide.BUY, timestamp);
    }
}
