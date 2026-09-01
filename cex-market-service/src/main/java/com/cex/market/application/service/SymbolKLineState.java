package com.cex.market.application.service;

import com.cex.market.domain.kline.KLineAggregationResult;
import com.cex.market.domain.kline.KLineAggregator;
import com.cex.market.domain.trade.MarketTrade;

import java.util.LinkedHashMap;
import java.util.Map;

/** 单交易对 KLine 聚合及持久化重试期间的待确认状态。 */
final class SymbolKLineState {

    private static final int MAX_PROCESSED_TRADE_IDS = 100_000;

    private final KLineAggregator aggregator;
    private final Map<String, Boolean> processedTradeIds = new LinkedHashMap<>();
    private final Map<String, KLineAggregationResult> pendingResults = new LinkedHashMap<>();

    SymbolKLineState(String symbol) {
        this.aggregator = new KLineAggregator(symbol);
    }

    KLinePreparation prepare(MarketTrade trade) {
        if (processedTradeIds.containsKey(trade.tradeId())) {
            return KLinePreparation.duplicateResult();
        }
        KLineAggregationResult pending = pendingResults.get(trade.tradeId());
        if (pending != null) {
            return KLinePreparation.pendingResult(pending);
        }
        KLineAggregationResult result = aggregator.apply(trade);
        pendingResults.put(trade.tradeId(), result);
        return KLinePreparation.pendingResult(result);
    }

    void confirm(String tradeId) {
        if (pendingResults.remove(tradeId) == null) {
            return;
        }
        processedTradeIds.put(tradeId, Boolean.TRUE);
        while (processedTradeIds.size() > MAX_PROCESSED_TRADE_IDS) {
            processedTradeIds.remove(processedTradeIds.keySet().iterator().next());
        }
    }

    record KLinePreparation(boolean duplicate, KLineAggregationResult result) {
        static KLinePreparation duplicateResult() {
            return new KLinePreparation(true, null);
        }

        static KLinePreparation pendingResult(KLineAggregationResult result) {
            return new KLinePreparation(false, result);
        }
    }
}
