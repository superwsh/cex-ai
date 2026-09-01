package com.cex.market.domain.trade;

import java.math.BigDecimal;

/** 单分钟内按成交顺序聚合的 Ticker 数据。 */
final class TickerMinuteBucket {

    private final long minute;
    private BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal close;
    private BigDecimal volume = BigDecimal.ZERO;
    private BigDecimal quoteVolume = BigDecimal.ZERO;
    private long tradeCount;

    TickerMinuteBucket(long minute) {
        this.minute = minute;
    }

    void add(MarketTrade trade) {
        if (open == null) {
            open = trade.price();
            high = trade.price();
            low = trade.price();
        } else {
            high = high.max(trade.price());
            low = low.min(trade.price());
        }
        close = trade.price();
        volume = volume.add(trade.quantity());
        quoteVolume = quoteVolume.add(trade.quoteQuantity());
        tradeCount++;
    }

    long minute() {
        return minute;
    }

    BigDecimal open() {
        return open;
    }

    BigDecimal high() {
        return high;
    }

    BigDecimal low() {
        return low;
    }

    BigDecimal close() {
        return close;
    }

    BigDecimal volume() {
        return volume;
    }

    BigDecimal quoteVolume() {
        return quoteVolume;
    }

    long tradeCount() {
        return tradeCount;
    }
}
