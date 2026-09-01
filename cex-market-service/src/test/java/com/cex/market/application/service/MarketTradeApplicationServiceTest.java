package com.cex.market.application.service;

import com.cex.common.kafka.event.TradeEvent;
import com.cex.common.kafka.event.market.PriceLevelChange;
import com.cex.market.domain.model.MarketOrderBook;
import com.cex.market.domain.model.MarketDepthSnapshot;
import com.cex.market.domain.trade.BookTicker;
import com.cex.market.domain.trade.MarketTrade;
import com.cex.market.domain.trade.Ticker24h;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** 成交应用编排、缓存写入和 BookTicker 生成测试。 */
class MarketTradeApplicationServiceTest {

    private static final String SYMBOL = "BTC_USDT";

    @Test
    void shouldAggregateTradeAndWriteHotCache() {
        RecordingMarketDataCache cache = new RecordingMarketDataCache();
        MarketTradeApplicationService service = new MarketTradeApplicationService(cache);

        MarketTradeProcessingResult result = service.process(trade("T-1", "100", "2"));

        assertThat(result.duplicate()).isFalse();
        assertThat(cache.tradeSnapshot).isNotNull();
        assertThat(cache.tradeSnapshot.lastPrice()).isEqualByComparingTo("100");
        assertThat(cache.tradeSnapshot.recentTrades()).hasSize(1);
        assertThat(cache.tradeSnapshot.ticker24h().tradeCount()).isEqualTo(1L);
    }

    @Test
    void shouldIgnoreDuplicateTradeAndKeepOneRecentTrade() {
        RecordingMarketDataCache cache = new RecordingMarketDataCache();
        MarketTradeApplicationService service = new MarketTradeApplicationService(cache);
        TradeEvent event = trade("T-1", "100", "2");
        service.process(event);

        MarketTradeProcessingResult result = service.process(event);

        assertThat(result.duplicate()).isTrue();
        assertThat(result.snapshot().recentTrades()).hasSize(1);
        assertThat(result.snapshot().ticker24h().tradeCount()).isEqualTo(1L);
    }

    @Test
    void shouldBuildBookTickerFromCurrentBestLevels() {
        RecordingMarketDataCache cache = new RecordingMarketDataCache();
        MarketOrderBook orderBook = new MarketOrderBook(SYMBOL);
        orderBook.loadSnapshot(10L, List.of(level("100", "2")), List.of(level("101", "3")));
        RecordingMarketDataPublisher publisher = new RecordingMarketDataPublisher();
        MarketBookTickerApplicationService service = new MarketBookTickerApplicationService(cache, publisher);

        BookTicker ticker = service.refresh(orderBook, 1_700_000_000_000L);

        assertThat(ticker.bidPrice()).isEqualByComparingTo("100");
        assertThat(ticker.askPrice()).isEqualByComparingTo("101");
        assertThat(ticker.sequence()).isEqualTo(10L);
        assertThat(cache.bookTicker).isEqualTo(ticker);
        assertThat(cache.depthSnapshot.sequence()).isEqualTo(10L);
        assertThat(publisher.bookTicker).isEqualTo(ticker);
        assertThat(publisher.depthSnapshot).isEqualTo(cache.depthSnapshot);
    }

    private TradeEvent trade(String tradeId, String price, String quantity) {
        BigDecimal decimalPrice = new BigDecimal(price);
        BigDecimal decimalQuantity = new BigDecimal(quantity);
        return TradeEvent.builder().eventVersion(1).eventId("event-" + tradeId).tradeId(tradeId).symbol(SYMBOL)
                .price(decimalPrice).quantity(decimalQuantity).amount(decimalPrice.multiply(decimalQuantity))
                .quoteQuantity(decimalPrice.multiply(decimalQuantity)).takerSide(TradeEvent.TakerSide.BUY)
                .timestamp(System.currentTimeMillis()).build();
    }

    private PriceLevelChange level(String price, String quantity) {
        return PriceLevelChange.builder().price(new BigDecimal(price)).quantity(new BigDecimal(quantity)).build();
    }

    private static final class RecordingMarketDataCache implements MarketDataCache {
        private MarketTradeCacheSnapshot tradeSnapshot;
        private BookTicker bookTicker;
        private MarketDepthSnapshot depthSnapshot;

        @Override
        public void saveTradeSnapshot(MarketTradeCacheSnapshot snapshot) {
            tradeSnapshot = snapshot;
        }

        @Override
        public void saveBookTicker(BookTicker ticker) {
            bookTicker = ticker;
        }

        @Override
        public void saveDepthSnapshot(MarketDepthSnapshot snapshot) {
            depthSnapshot = snapshot;
        }

        @Override
        public List<String> findSymbols() {
            return List.of();
        }

        @Override
        public List<MarketTrade> findRecentTrades(String symbol, int limit) {
            return List.of();
        }

        @Override
        public Ticker24h findTicker24h(String symbol) {
            return null;
        }

        @Override
        public BookTicker findBookTicker(String symbol) {
            return null;
        }

        @Override
        public MarketDepthSnapshot findDepthSnapshot(String symbol, int limit) {
            return null;
        }
    }

    private static final class RecordingMarketDataPublisher implements MarketDataPublisher {
        private BookTicker bookTicker;
        private MarketDepthSnapshot depthSnapshot;

        @Override
        public void publishTrade(com.cex.market.domain.trade.MarketTrade trade) {
        }

        @Override
        public void publishTicker(Ticker24h ticker) {
        }

        @Override
        public void publishBookTicker(BookTicker ticker) {
            bookTicker = ticker;
        }

        @Override
        public void publishDepth(MarketDepthSnapshot snapshot) {
            depthSnapshot = snapshot;
        }

        @Override
        public void publishKLine(com.cex.market.domain.kline.KLine kLine) {
        }
    }
}
