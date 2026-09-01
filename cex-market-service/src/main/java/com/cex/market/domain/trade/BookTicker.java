package com.cex.market.domain.trade;

import java.math.BigDecimal;

/** 一个交易对在指定盘口序号上的最佳买卖报价。 */
public record BookTicker(String symbol, BigDecimal bidPrice, BigDecimal bidQuantity,
                         BigDecimal askPrice, BigDecimal askQuantity, long sequence, long timestamp) {

    /**
     * 创建最佳买卖报价；任一侧为空表示该侧当前没有挂单。
     *
     * @param symbol 交易对
     * @param bidPrice 最佳买价
     * @param bidQuantity 最佳买量
     * @param askPrice 最佳卖价
     * @param askQuantity 最佳卖量
     * @param sequence 对应盘口序号
     * @param timestamp 更新时间（毫秒时间戳）
     */
    public BookTicker {
        if (symbol == null || symbol.isBlank() || sequence < 0 || timestamp <= 0
                || !validSide(bidPrice, bidQuantity) || !validSide(askPrice, askQuantity)) {
            throw new IllegalArgumentException("最佳买卖报价字段非法");
        }
    }

    private static boolean validSide(BigDecimal price, BigDecimal quantity) {
        if (price == null || quantity == null) {
            return price == null && quantity == null;
        }
        return price.signum() > 0 && quantity.signum() > 0;
    }
}
