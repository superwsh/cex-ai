package com.cex.market.domain.trade;

import com.cex.common.kafka.event.TradeEvent;

import java.math.BigDecimal;

/** 面向行情的不可变逐笔成交记录。 */
public record MarketTrade(String tradeId, String symbol, BigDecimal price, BigDecimal quantity,
                          BigDecimal quoteQuantity, TradeEvent.TakerSide side, long timestamp) {

    /**
     * 创建并校验逐笔成交记录。
     *
     * @param tradeId 成交编号
     * @param symbol 交易对
     * @param price 成交价格
     * @param quantity 成交数量
     * @param quoteQuantity 计价成交量
     * @param side 主动方方向
     * @param timestamp 成交时间（毫秒时间戳）
     */
    public MarketTrade {
        if (tradeId == null || tradeId.isBlank() || symbol == null || symbol.isBlank() || side == null
                || timestamp <= 0 || nonPositive(price) || nonPositive(quantity) || nonPositive(quoteQuantity)
                || price.multiply(quantity).compareTo(quoteQuantity) != 0) {
            throw new IllegalArgumentException("行情成交字段非法");
        }
    }

    private static boolean nonPositive(BigDecimal value) {
        return value == null || value.signum() <= 0;
    }
}
