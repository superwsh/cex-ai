package com.cex.market.application.service;

import com.cex.common.kafka.event.TradeEvent;
import com.cex.market.domain.trade.Ticker24h;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.Objects;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/** 编排逐笔成交消费、内存聚合与 Redis 热数据更新。 */
@Service
@RequiredArgsConstructor
public class MarketTradeApplicationService {

    private final MarketDataCache marketDataCache;
    private final ConcurrentHashMap<String, SymbolTradeState> symbolStates = new ConcurrentHashMap<>();
    private final Clock clock = Clock.systemUTC();

    /**
     * 处理一笔成交事件，并在聚合成功后写入热点缓存。
     *
     * @param event Kafka 投递的成交事件
     * @return 本次事件的幂等处理结果
     */
    public MarketTradeProcessingResult process(TradeEvent event) {
        Objects.requireNonNull(event, "成交事件不能为空");
        if (event.getSymbol() == null || event.getSymbol().isBlank()) {
            throw new IllegalArgumentException("成交事件交易对不能为空");
        }
        AtomicReference<MarketTradeProcessingResult> resultReference = new AtomicReference<>();
        symbolStates.compute(event.getSymbol(), (symbol, currentState) -> {
            SymbolTradeState state = currentState == null ? new SymbolTradeState(symbol) : currentState;
            resultReference.set(state.process(event, clock.millis()));
            return state;
        });
        MarketTradeProcessingResult result = resultReference.get();
        marketDataCache.saveTradeSnapshot(result.snapshot());
        return result;
    }

    /**
     * 按当前时间推进所有已活跃交易对的 24 小时窗口，并刷新 Redis 热缓存。
     */
    public List<Ticker24h> refreshTickerCaches() {
        long now = clock.millis();
        List<MarketTradeCacheSnapshot> snapshots = new ArrayList<>();
        symbolStates.forEach((symbol, ignored) -> symbolStates.computeIfPresent(symbol, (key, state) -> {
            snapshots.add(state.refreshTicker(now));
            return state;
        }));
        snapshots.forEach(marketDataCache::saveTradeSnapshot);
        return snapshots.stream().map(MarketTradeCacheSnapshot::ticker24h).filter(ticker -> ticker != null).toList();
    }
}
