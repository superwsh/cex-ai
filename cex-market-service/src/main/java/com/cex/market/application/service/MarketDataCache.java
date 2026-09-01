package com.cex.market.application.service;

import com.cex.market.domain.model.MarketDepthSnapshot;
import com.cex.market.domain.trade.BookTicker;
import com.cex.market.domain.trade.MarketTrade;
import com.cex.market.domain.trade.Ticker24h;

import java.util.List;

/** 行情热点数据缓存端口，缓存不可作为行情事实来源。 */
public interface MarketDataCache {

    /**
     * 写入一个交易对的成交、最新价和 Ticker 热数据。
     *
     * @param snapshot 待缓存的完整成交行情快照
     */
    void saveTradeSnapshot(MarketTradeCacheSnapshot snapshot);

    /**
     * 写入一个交易对的最佳买卖报价。
     *
     * @param ticker 待缓存的最佳买卖报价
     */
    void saveBookTicker(BookTicker ticker);

    /**
     * 写入一个交易对的盘口深度快照。
     *
     * @param snapshot 已校验的盘口深度快照
     */
    void saveDepthSnapshot(MarketDepthSnapshot snapshot);

    /**
     * 查询已产生行情数据的交易对。
     *
     * @return 不可修改的交易对列表
     */
    List<String> findSymbols();

    /**
     * 查询一个交易对的最近成交。
     *
     * @param symbol 交易对
     * @param limit 最大返回数量
     * @return 按成交时间倒序排列的成交记录；缓存不存在时返回空列表
     */
    List<MarketTrade> findRecentTrades(String symbol, int limit);

    /**
     * 查询一个交易对的 24 小时行情。
     *
     * @param symbol 交易对
     * @return 24 小时行情；缓存不存在时返回空
     */
    Ticker24h findTicker24h(String symbol);

    /**
     * 查询一个交易对的最佳买卖报价。
     *
     * @param symbol 交易对
     * @return 最佳买卖报价；缓存不存在时返回空
     */
    BookTicker findBookTicker(String symbol);

    /**
     * 查询一个交易对的盘口深度快照。
     *
     * @param symbol 交易对
     * @param limit 买卖两侧最大档位数
     * @return 截断后的深度快照；缓存不存在时返回空
     */
    MarketDepthSnapshot findDepthSnapshot(String symbol, int limit);
}
