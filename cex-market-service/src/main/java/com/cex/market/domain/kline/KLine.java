package com.cex.market.domain.kline;

import java.math.BigDecimal;

/** 一个交易对在固定 UTC 时间窗口内的 OHLCV 行情。 */
public record KLine(String symbol, KLineInterval interval, long openTime, long closeTime,
                    BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close,
                    BigDecimal volume, BigDecimal quoteVolume, long tradeCount, boolean closed) {

    /**
     * 创建已校验的 KLine。
     *
     * @param symbol 交易对
     * @param interval 周期
     * @param openTime UTC 窗口开始时间
     * @param closeTime UTC 窗口结束时间
     * @param open 开盘价
     * @param high 最高价
     * @param low 最低价
     * @param close 收盘价
     * @param volume 基础资产成交量
     * @param quoteVolume 计价资产成交量
     * @param tradeCount 成交笔数
     * @param closed 是否已收线
     */
    public KLine {
        if (symbol == null || symbol.isBlank() || interval == null || openTime < 0 || closeTime < openTime
                || tradeCount <= 0 || nonPositive(open) || nonPositive(high) || nonPositive(low) || nonPositive(close)
                || nonPositive(volume) || nonPositive(quoteVolume) || high.compareTo(low) < 0
                || open.compareTo(high) > 0 || open.compareTo(low) < 0 || close.compareTo(high) > 0
                || close.compareTo(low) < 0) {
            throw new IllegalArgumentException("KLine 字段非法");
        }
    }

    private static boolean nonPositive(BigDecimal value) {
        return value == null || value.signum() <= 0;
    }
}
