package com.cex.market.application.service;

import com.cex.market.domain.model.MarketOrderBook;
import com.cex.market.domain.model.MarketPriceLevel;
import com.cex.market.domain.trade.BookTicker;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/** 从聚合盘口提取并缓存最佳买卖报价。 */
@Service
@RequiredArgsConstructor
public class MarketBookTickerApplicationService {

    private static final int MAX_DEPTH_LEVELS = 500;

    private final MarketDataCache marketDataCache;
    private final MarketDataPublisher marketDataPublisher;

    /**
     * 根据指定盘口的当前最优档位创建并缓存 BookTicker。
     *
     * @param orderBook 已应用连续增量的聚合盘口
     * @param timestamp 报价生成时间（毫秒时间戳）
     * @return 当前最佳买卖报价
     */
    public BookTicker refresh(MarketOrderBook orderBook, long timestamp) {
        if (orderBook == null || timestamp <= 0) {
            throw new IllegalArgumentException("订单簿或报价时间非法");
        }
        MarketPriceLevel bid = orderBook.bestBid().orElse(null);
        MarketPriceLevel ask = orderBook.bestAsk().orElse(null);
        BookTicker ticker = new BookTicker(orderBook.getSymbol(), bid == null ? null : bid.price(),
                bid == null ? null : bid.quantity(), ask == null ? null : ask.price(),
                ask == null ? null : ask.quantity(), orderBook.getSequence(), timestamp);
        marketDataCache.saveBookTicker(ticker);
        var depthSnapshot = orderBook.depthSnapshot(MAX_DEPTH_LEVELS, timestamp);
        marketDataCache.saveDepthSnapshot(depthSnapshot);
        marketDataPublisher.publishBookTicker(ticker);
        marketDataPublisher.publishDepth(depthSnapshot);
        return ticker;
    }
}
