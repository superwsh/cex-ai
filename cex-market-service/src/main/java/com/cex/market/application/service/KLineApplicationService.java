package com.cex.market.application.service;

import com.cex.market.domain.kline.KLineAggregationResult;
import com.cex.market.domain.kline.KLine;
import com.cex.market.domain.trade.MarketTrade;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.List;

/** 编排 KLine 聚合、当前缓存与已收线持久化。 */
@Service
@RequiredArgsConstructor
public class KLineApplicationService {

    private final KLineRepository kLineRepository;
    private final KLineCache kLineCache;
    private final ConcurrentHashMap<String, SymbolKLineState> states = new ConcurrentHashMap<>();

    /**
     * 幂等处理一笔成交的全部 KLine 周期。
     *
     * @param trade 已校验的逐笔成交
     */
    public List<KLine> process(MarketTrade trade) {
        AtomicReference<SymbolKLineState.KLinePreparation> preparationReference = new AtomicReference<>();
        states.compute(trade.symbol(), (symbol, currentState) -> {
            SymbolKLineState state = currentState == null ? new SymbolKLineState(symbol) : currentState;
            preparationReference.set(state.prepare(trade));
            return state;
        });
        SymbolKLineState.KLinePreparation preparation = preparationReference.get();
        if (preparation.duplicate()) {
            return List.of();
        }
        KLineAggregationResult result = preparation.result();
        result.closedKlines().forEach(kLineRepository::upsertClosed);
        result.currentKlines().forEach(kLineCache::saveCurrent);
        states.computeIfPresent(trade.symbol(), (symbol, state) -> {
            state.confirm(trade.tradeId());
            return state;
        });
        return result.currentKlines();
    }
}
