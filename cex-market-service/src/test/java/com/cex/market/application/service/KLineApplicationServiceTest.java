package com.cex.market.application.service;

import com.cex.common.kafka.event.TradeEvent;
import com.cex.market.domain.kline.KLine;
import com.cex.market.domain.kline.KLineInterval;
import com.cex.market.domain.trade.MarketTrade;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** KLine 应用服务的持久化、缓存和幂等测试。 */
class KLineApplicationServiceTest {

    private static final String SYMBOL = "BTC_USDT";

    @Test
    void shouldPersistClosedKlinesAndCacheCurrentKlines() {
        RecordingKLineRepository repository = new RecordingKLineRepository();
        RecordingKLineCache cache = new RecordingKLineCache();
        KLineApplicationService service = new KLineApplicationService(repository, cache);

        service.process(trade("T-1", "100", "2", 60_001L));
        service.process(trade("T-2", "110", "1", 120_001L));

        assertThat(repository.closed).hasSize(1).allMatch(KLine::closed);
        assertThat(cache.current.values()).hasSize(6).allMatch(kLine -> !kLine.closed());
    }

    @Test
    void shouldNotPersistOrCacheAgainForDuplicateTrade() {
        RecordingKLineRepository repository = new RecordingKLineRepository();
        RecordingKLineCache cache = new RecordingKLineCache();
        KLineApplicationService service = new KLineApplicationService(repository, cache);
        MarketTrade trade = trade("T-1", "100", "2", 60_001L);

        service.process(trade);
        int cachedCount = cache.current.size();
        service.process(trade);

        assertThat(repository.closed).isEmpty();
        assertThat(cache.current).hasSize(cachedCount);
    }

    @Test
    void shouldReusePendingAggregationAfterCacheFailureWithoutDoubleCounting() {
        RecordingKLineRepository repository = new RecordingKLineRepository();
        FailingOnceKLineCache cache = new FailingOnceKLineCache();
        KLineApplicationService service = new KLineApplicationService(repository, cache);
        MarketTrade trade = trade("T-1", "100", "2", 60_001L);

        assertThatThrownBy(() -> service.process(trade)).isInstanceOf(IllegalStateException.class);
        service.process(trade);

        assertThat(cache.current.get(KLineInterval.ONE_MINUTE).tradeCount()).isEqualTo(1L);
    }

    private MarketTrade trade(String tradeId, String price, String quantity, long timestamp) {
        BigDecimal decimalPrice = new BigDecimal(price);
        BigDecimal decimalQuantity = new BigDecimal(quantity);
        return new MarketTrade(tradeId, SYMBOL, decimalPrice, decimalQuantity,
                decimalPrice.multiply(decimalQuantity), TradeEvent.TakerSide.BUY, timestamp);
    }

    private static final class RecordingKLineRepository implements KLineRepository {
        private final List<KLine> closed = new ArrayList<>();

        @Override
        public void upsertClosed(KLine kLine) {
            closed.add(kLine);
        }

        @Override
        public List<KLine> findClosed(String symbol, KLineInterval interval, Long startTime, Long endTime, int limit) {
            return List.of();
        }
    }

    private static class RecordingKLineCache implements KLineCache {
        protected final Map<KLineInterval, KLine> current = new EnumMap<>(KLineInterval.class);

        @Override
        public void saveCurrent(KLine kLine) {
            current.put(kLine.interval(), kLine);
        }

        @Override
        public KLine getCurrent(String symbol, KLineInterval interval) {
            throw new AssertionError("KLine 聚合测试不应读取缓存");
        }
    }

    private static final class FailingOnceKLineCache extends RecordingKLineCache {
        private boolean first = true;

        @Override
        public void saveCurrent(KLine kLine) {
            if (first) {
                first = false;
                throw new IllegalStateException("模拟 Redis 短暂故障");
            }
            super.saveCurrent(kLine);
        }
    }
}
