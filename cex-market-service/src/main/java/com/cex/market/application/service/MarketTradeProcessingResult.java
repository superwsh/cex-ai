package com.cex.market.application.service;

import com.cex.market.domain.trade.MarketTrade;

/** 一笔成交事件在行情核心中的处理结果。 */
public record MarketTradeProcessingResult(boolean duplicate, MarketTrade trade, MarketTradeCacheSnapshot snapshot) {

    /**
     * 创建成交处理结果。
     *
     * @param duplicate 是否为已处理的重复事件
     * @param trade 已校验的逐笔成交
     * @param snapshot 处理后应写入缓存的行情快照
     */
    public MarketTradeProcessingResult {
        if (trade == null || snapshot == null) {
            throw new IllegalArgumentException("成交处理结果字段不能为空");
        }
    }
}
