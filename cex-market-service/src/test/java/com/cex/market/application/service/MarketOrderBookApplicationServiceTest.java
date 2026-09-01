package com.cex.market.application.service;

import com.cex.common.kafka.event.market.OrderBookDeltaEvent;
import com.cex.common.kafka.event.market.PriceLevelChange;
import com.cex.market.domain.model.MarketDepthSnapshot;
import com.cex.market.domain.model.MarketOrderBookSnapshot;
import com.cex.market.domain.trade.BookTicker;
import com.cex.market.domain.trade.MarketTrade;
import com.cex.market.domain.trade.Ticker24h;
import com.cex.market.infrastructure.metrics.MarketMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** 订单簿快照、启动恢复和 Kafka 重放应用服务测试。 */
class MarketOrderBookApplicationServiceTest {

    private static final String SYMBOL = "BTC_USDT";

    @Test
    void shouldPersistSnapshotBeforePublishingBookTicker() {
        InMemorySnapshotRepository repository = new InMemorySnapshotRepository();
        RecordingMarketDataCache cache = new RecordingMarketDataCache();
        MarketOrderBookApplicationService service = service(repository, new RecordingReplayer(), cache);

        MarketOrderBookProcessingResult result = service.process(delta("event-1", 1L, 0L, "100", "2"), 0, 5L);

        assertThat(result.recovered()).isFalse();
        assertThat(repository.findBySymbol(SYMBOL).sequence()).isEqualTo(1L);
        assertThat(cache.bookTicker.sequence()).isEqualTo(1L);
        assertThat(cache.depthSnapshot.bids()).hasSize(1);
    }

    @Test
    void shouldRestorePersistedSnapshotOnStartupAndApplyNextDelta() {
        InMemorySnapshotRepository repository = new InMemorySnapshotRepository();
        RecordingMarketDataCache firstCache = new RecordingMarketDataCache();
        MarketOrderBookApplicationService firstService = service(repository, new RecordingReplayer(), firstCache);
        firstService.process(delta("event-1", 1L, 0L, "100", "2"), 0, 5L);

        RecordingMarketDataCache restartedCache = new RecordingMarketDataCache();
        MarketOrderBookApplicationService restartedService = service(repository, new RecordingReplayer(), restartedCache);
        restartedService.restoreSnapshots();
        MarketOrderBookProcessingResult result = restartedService.process(delta("event-2", 2L, 1L, "101", "3"), 0, 6L);

        assertThat(result.recovered()).isFalse();
        assertThat(repository.findBySymbol(SYMBOL).sequence()).isEqualTo(2L);
        assertThat(restartedCache.bookTicker.bidPrice()).isEqualByComparingTo("101");
    }

    @Test
    void shouldReplayFromSnapshotWhenSequenceGapDetected() {
        InMemorySnapshotRepository repository = new InMemorySnapshotRepository();
        RecordingMarketDataCache cache = new RecordingMarketDataCache();
        MarketOrderBookApplicationService initialService = service(repository, new RecordingReplayer(), cache);
        initialService.process(delta("event-1", 1L, 0L, "100", "2"), 0, 5L);
        OrderBookDeltaEvent second = delta("event-2", 2L, 1L, "101", "3");
        OrderBookDeltaEvent third = delta("event-3", 3L, 2L, "102", "4");
        RecordingReplayer replayer = new RecordingReplayer(List.of(second, third));
        MarketOrderBookApplicationService service = service(repository, replayer, cache);
        service.restoreSnapshots();

        MarketOrderBookProcessingResult result = service.process(third, 0, 7L);

        assertThat(result.recovered()).isTrue();
        assertThat(repository.findBySymbol(SYMBOL).sequence()).isEqualTo(3L);
        assertThat(replayer.called).isTrue();
        assertThat(cache.bookTicker.bidPrice()).isEqualByComparingTo("102");
    }

    private MarketOrderBookApplicationService service(InMemorySnapshotRepository repository,
                                                       RecordingReplayer replayer, RecordingMarketDataCache cache) {
        return new MarketOrderBookApplicationService(repository, replayer,
                new MarketBookTickerApplicationService(cache, new NoopMarketDataPublisher()),
                new MarketMetrics(new SimpleMeterRegistry()));
    }

    private OrderBookDeltaEvent delta(String eventId, long sequence, long previousSequence, String price, String quantity) {
        return OrderBookDeltaEvent.builder().eventId(eventId).eventVersion(1).symbol(SYMBOL).sequence(sequence)
                .previousSequence(previousSequence).bids(List.of(level(price, quantity))).asks(List.of())
                .eventTime(1_700_000_000_000L + sequence).build();
    }

    private PriceLevelChange level(String price, String quantity) {
        return PriceLevelChange.builder().price(new BigDecimal(price)).quantity(new BigDecimal(quantity)).build();
    }

    private static final class InMemorySnapshotRepository implements MarketOrderBookSnapshotRepository {
        private final Map<String, MarketOrderBookSnapshot> snapshots = new HashMap<>();

        @Override
        public void save(MarketOrderBookSnapshot snapshot) {
            snapshots.compute(snapshot.symbol(), (symbol, current) -> current == null || snapshot.sequence() >= current.sequence()
                    ? snapshot : current);
        }

        @Override
        public MarketOrderBookSnapshot findBySymbol(String symbol) {
            return snapshots.get(symbol);
        }

        @Override
        public List<MarketOrderBookSnapshot> findAll() {
            return List.copyOf(snapshots.values());
        }
    }

    private static final class RecordingReplayer implements OrderBookEventReplayer {
        private final List<OrderBookDeltaEvent> events;
        private boolean called;

        private RecordingReplayer() {
            this(List.of());
        }

        private RecordingReplayer(List<OrderBookDeltaEvent> events) {
            this.events = events;
        }

        @Override
        public List<OrderBookDeltaEvent> replayTo(MarketOrderBookSnapshot snapshot, int targetPartition, long targetOffset) {
            called = true;
            return events;
        }
    }

    private static final class RecordingMarketDataCache implements MarketDataCache {
        private BookTicker bookTicker;
        private MarketDepthSnapshot depthSnapshot;

        @Override
        public void saveTradeSnapshot(MarketTradeCacheSnapshot snapshot) {
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

    private static final class NoopMarketDataPublisher implements MarketDataPublisher {
        @Override
        public void publishTrade(MarketTrade trade) {
        }

        @Override
        public void publishTicker(Ticker24h ticker) {
        }

        @Override
        public void publishBookTicker(BookTicker ticker) {
        }

        @Override
        public void publishDepth(MarketDepthSnapshot snapshot) {
        }

        @Override
        public void publishKLine(com.cex.market.domain.kline.KLine kLine) {
        }
    }
}
