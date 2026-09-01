package com.cex.market.application.service;

import com.cex.market.domain.trade.MarketTrade;
import com.cex.market.domain.trade.Ticker24h;

import java.math.BigDecimal;
import java.util.List;

/** 需同步写入 Redis 的单交易对成交行情快照。 */
public record MarketTradeCacheSnapshot(String symbol, List<MarketTrade> recentTrades,
                                       BigDecimal lastPrice, Ticker24h ticker24h) {

    /**
     * 创建不可变的成交行情缓存快照。
     *
     * @param symbol 交易对
     * @param recentTrades 最近成交，按时间倒序
     * @param lastPrice 最新成交价
     * @param ticker24h 最近 24 小时行情；尚无窗口内成交时为空
     */
    public MarketTradeCacheSnapshot {
        if (symbol == null || symbol.isBlank() || recentTrades == null || lastPrice == null) {
            throw new IllegalArgumentException("成交行情缓存快照字段非法");
        }
        recentTrades = List.copyOf(recentTrades);
    }
}
