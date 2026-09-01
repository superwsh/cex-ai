package com.cex.market.application.service;

import com.cex.market.domain.kline.KLine;
import com.cex.market.domain.model.MarketDepthSnapshot;
import com.cex.market.domain.trade.BookTicker;
import com.cex.market.domain.trade.MarketTrade;
import com.cex.market.domain.trade.Ticker24h;

/** 行情读取模型变更后的异步发布端口。 */
public interface MarketDataPublisher {

    /**
     * 发布逐笔成交。
     *
     * @param trade 已完成聚合的成交
     */
    void publishTrade(MarketTrade trade);

    /**
     * 发布 24 小时 Ticker。
     *
     * @param ticker 已完成聚合的 Ticker
     */
    void publishTicker(Ticker24h ticker);

    /**
     * 发布最佳买卖报价。
     *
     * @param ticker 最新最佳买卖报价
     */
    void publishBookTicker(BookTicker ticker);

    /**
     * 发布盘口深度快照。
     *
     * @param snapshot 最新盘口深度快照
     */
    void publishDepth(MarketDepthSnapshot snapshot);

    /**
     * 发布当前 KLine。
     *
     * @param kLine 最新 KLine
     */
    void publishKLine(KLine kLine);
}
