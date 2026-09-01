package com.cex.market.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.common.kafka.event.TradeEvent;
import com.cex.market.domain.model.MarketDepthSnapshot;
import com.cex.market.domain.model.MarketPriceLevel;
import com.cex.market.domain.trade.BookTicker;
import com.cex.market.domain.trade.MarketTrade;
import com.cex.market.domain.trade.Ticker24h;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 市场 REST 查询应用服务测试。 */
class MarketQueryServiceTest {

    private static final String SYMBOL = "BTC_USDT";

    @Test
    void shouldReturnTickerAndBookTickerForRequestedSymbol() {
        InMemoryMarketDataCache cache = new InMemoryMarketDataCache();
        cache.ticker = ticker();
        cache.bookTicker = bookTicker();
        MarketQueryService service = new MarketQueryService(cache);

        assertThat(service.ticker24h(SYMBOL)).isEqualTo(cache.ticker);
        assertThat(service.bookTicker(SYMBOL)).isEqualTo(cache.bookTicker);
    }

    @Test
    void shouldRejectUnsupportedDepthLimit() {
        MarketQueryService service = new MarketQueryService(new InMemoryMarketDataCache());

        assertThatThrownBy(() -> service.depth(SYMBOL, 30))
                .isInstanceOf(BizException.class)
                .extracting(exception -> ((BizException) exception).getCode())
                .isEqualTo(400);
    }

    @Test
    void shouldRejectTradeLimitExceedingMaximum() {
        MarketQueryService service = new MarketQueryService(new InMemoryMarketDataCache());

        assertThatThrownBy(() -> service.trades(SYMBOL, 1_001))
                .isInstanceOf(BizException.class)
                .extracting(exception -> ((BizException) exception).getCode())
                .isEqualTo(400);
    }

    @Test
    void shouldReturnDepthAndRecentTradesFromCache() {
        InMemoryMarketDataCache cache = new InMemoryMarketDataCache();
        cache.depth = new MarketDepthSnapshot(SYMBOL, 12L,
                List.of(new MarketPriceLevel(new BigDecimal("100"), new BigDecimal("2"))),
                List.of(new MarketPriceLevel(new BigDecimal("101"), new BigDecimal("3"))), 1_700_000_000_000L);
        cache.trades = List.of(trade());
        MarketQueryService service = new MarketQueryService(cache);

        assertThat(service.depth(SYMBOL, 20)).isEqualTo(cache.depth);
        assertThat(service.trades(SYMBOL, 100)).containsExactly(trade());
    }

    @Test
    void shouldReturnNotReadyWhenSingleTickerIsAbsent() {
        MarketQueryService service = new MarketQueryService(new InMemoryMarketDataCache());

        assertThatThrownBy(() -> service.ticker24h(SYMBOL))
                .isInstanceOf(BizException.class)
                .extracting(exception -> ((BizException) exception).getCode())
                .isEqualTo(404);
    }

    @Test
    void shouldRejectBlankOptionalTickerSymbol() {
        MarketQueryService service = new MarketQueryService(new InMemoryMarketDataCache());

        assertThatThrownBy(() -> service.ticker24h(" "))
                .isInstanceOf(BizException.class)
                .extracting(exception -> ((BizException) exception).getCode())
                .isEqualTo(400);
    }

    private static Ticker24h ticker() {
        return new Ticker24h(SYMBOL, new BigDecimal("100"), new BigDecimal("90"), new BigDecimal("110"),
                new BigDecimal("80"), new BigDecimal("10"), new BigDecimal("11.11"), new BigDecimal("12"),
                new BigDecimal("1200"), 2L, 1_699_913_600_000L, 1_700_000_000_000L);
    }

    private static BookTicker bookTicker() {
        return new BookTicker(SYMBOL, new BigDecimal("100"), new BigDecimal("2"), new BigDecimal("101"),
                new BigDecimal("3"), 12L, 1_700_000_000_000L);
    }

    private static MarketTrade trade() {
        return new MarketTrade("trade-1", SYMBOL, new BigDecimal("100"), new BigDecimal("2"),
                new BigDecimal("200"), TradeEvent.TakerSide.BUY, 1_700_000_000_000L);
    }

    private static final class InMemoryMarketDataCache implements MarketDataCache {
        private List<MarketTrade> trades = List.of();
        private Ticker24h ticker;
        private BookTicker bookTicker;
        private MarketDepthSnapshot depth;

        @Override
        public void saveTradeSnapshot(MarketTradeCacheSnapshot snapshot) {
        }

        @Override
        public void saveBookTicker(BookTicker ticker) {
            bookTicker = ticker;
        }

        @Override
        public void saveDepthSnapshot(MarketDepthSnapshot snapshot) {
            depth = snapshot;
        }

        @Override
        public List<String> findSymbols() {
            return List.of(SYMBOL);
        }

        @Override
        public List<MarketTrade> findRecentTrades(String symbol, int limit) {
            return trades.stream().limit(limit).toList();
        }

        @Override
        public Ticker24h findTicker24h(String symbol) {
            return ticker;
        }

        @Override
        public BookTicker findBookTicker(String symbol) {
            return bookTicker;
        }

        @Override
        public MarketDepthSnapshot findDepthSnapshot(String symbol, int limit) {
            return depth;
        }
    }
}
