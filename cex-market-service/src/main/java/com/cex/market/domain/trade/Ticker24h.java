package com.cex.market.domain.trade;

import java.math.BigDecimal;

/** 最近 24 小时滚动窗口的行情统计。 */
public record Ticker24h(String symbol, BigDecimal lastPrice, BigDecimal openPrice, BigDecimal highPrice,
                        BigDecimal lowPrice, BigDecimal priceChange, BigDecimal priceChangePercent,
                        BigDecimal volume, BigDecimal quoteVolume, long tradeCount, long openTime, long closeTime) {

    /**
     * 创建可对外读取的 24 小时行情统计。
     *
     * @param symbol 交易对
     * @param lastPrice 最新成交价
     * @param openPrice 窗口开盘价
     * @param highPrice 窗口最高价
     * @param lowPrice 窗口最低价
     * @param priceChange 价格涨跌额
     * @param priceChangePercent 价格涨跌幅百分比
     * @param volume 基础资产成交量
     * @param quoteVolume 计价资产成交量
     * @param tradeCount 成交笔数
     * @param openTime 窗口第一分钟开始时间
     * @param closeTime 计算时间
     */
    public Ticker24h {
        if (symbol == null || symbol.isBlank() || tradeCount <= 0 || openTime <= 0 || closeTime <= 0
                || lastPrice == null || openPrice == null || highPrice == null || lowPrice == null
                || priceChange == null || priceChangePercent == null || volume == null || quoteVolume == null) {
            throw new IllegalArgumentException("24 小时行情字段非法");
        }
    }
}
