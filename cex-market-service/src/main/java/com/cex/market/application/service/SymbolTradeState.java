package com.cex.market.application.service;

import com.cex.common.kafka.event.TradeEvent;
import com.cex.market.domain.trade.MarketTrade;
import com.cex.market.domain.trade.Ticker24h;
import com.cex.market.domain.trade.Ticker24hRollingWindow;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 同一交易对在单一 Kafka 分区消费顺序下维护的成交行情状态。 */
final class SymbolTradeState {

    private static final int MAX_RECENT_TRADES = 1_000;
    private static final int MAX_PROCESSED_TRADE_IDS = 100_000;

    private final String symbol;
    private final Ticker24hRollingWindow tickerWindow;
    private final Deque<MarketTrade> recentTrades = new ArrayDeque<>();
    private final Map<String, Boolean> processedTradeIds = new LinkedHashMap<>();
    private BigDecimal lastPrice;
    private Ticker24h ticker24h;

    SymbolTradeState(String symbol) {
        this.symbol = symbol;
        this.tickerWindow = new Ticker24hRollingWindow(symbol);
    }

    MarketTradeProcessingResult process(TradeEvent event, long now) {
        MarketTrade trade = toMarketTrade(event);
        if (processedTradeIds.containsKey(trade.tradeId())) {
            return new MarketTradeProcessingResult(true, trade, snapshot());
        }
        processedTradeIds.put(trade.tradeId(), Boolean.TRUE);
        trimProcessedTradeIds();
        recentTrades.addFirst(trade);
        while (recentTrades.size() > MAX_RECENT_TRADES) {
            recentTrades.removeLast();
        }
        lastPrice = trade.price();
        tickerWindow.add(trade, now).ifPresent(ticker -> ticker24h = ticker);
        return new MarketTradeProcessingResult(false, trade, snapshot());
    }

    /**
     * 推进 24 小时滚动窗口并生成当前缓存快照。
     *
     * @param now 当前计算时间（毫秒时间戳）
     * @return 更新后的缓存快照
     */
    MarketTradeCacheSnapshot refreshTicker(long now) {
        ticker24h = tickerWindow.current(now).orElse(null);
        return snapshot();
    }

    private MarketTradeCacheSnapshot snapshot() {
        return new MarketTradeCacheSnapshot(symbol, List.copyOf(recentTrades), lastPrice, ticker24h);
    }

    private MarketTrade toMarketTrade(TradeEvent event) {
        if (event == null || event.getEventVersion() == null || event.getEventVersion() != 1
                || event.getTradeId() == null || event.getTradeId().isBlank()
                || event.getSymbol() == null || event.getSymbol().isBlank()
                || event.getTimestamp() == null || event.getTakerSide() == null) {
            throw new IllegalArgumentException("成交事件缺少行情所需字段");
        }
        BigDecimal quoteQuantity = event.getQuoteQuantity() == null ? event.getAmount() : event.getQuoteQuantity();
        if (event.getAmount() != null && quoteQuantity != null && event.getAmount().compareTo(quoteQuantity) != 0) {
            throw new IllegalArgumentException("成交金额与计价成交量不一致");
        }
        return new MarketTrade(event.getTradeId(), event.getSymbol(), event.getPrice(), event.getQuantity(),
                quoteQuantity, event.getTakerSide(), event.getTimestamp());
    }

    private void trimProcessedTradeIds() {
        while (processedTradeIds.size() > MAX_PROCESSED_TRADE_IDS) {
            String oldestTradeId = processedTradeIds.keySet().iterator().next();
            processedTradeIds.remove(oldestTradeId);
        }
    }
}
