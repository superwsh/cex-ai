package com.cex.market.domain.kline;

import com.cex.market.domain.trade.MarketTrade;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** 按单交易对成交顺序聚合全部支持周期的 KLine。 */
public final class KLineAggregator {

    private final String symbol;
    private final Map<KLineInterval, MutableKLine> currentKlines = new EnumMap<>(KLineInterval.class);

    /**
     * 创建指定交易对的 KLine 聚合器。
     *
     * @param symbol 交易对
     */
    public KLineAggregator(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        this.symbol = symbol;
    }

    /**
     * 应用一笔按交易对顺序到达的成交。
     *
     * @param trade 逐笔成交
     * @return 全部当前 KLine 和本次收线 KLine
     */
    public KLineAggregationResult apply(MarketTrade trade) {
        if (!symbol.equals(trade.symbol())) {
            throw new IllegalArgumentException("成交交易对与 KLine 聚合器不一致");
        }
        List<KLine> closed = new ArrayList<>();
        for (KLineInterval interval : KLineInterval.values()) {
            long openTime = interval.openTimeOf(trade.timestamp());
            MutableKLine current = currentKlines.get(interval);
            if (current == null) {
                current = new MutableKLine(symbol, interval, openTime, trade);
                currentKlines.put(interval, current);
            } else if (openTime > current.openTime) {
                closed.add(current.toKLine(true));
                current = new MutableKLine(symbol, interval, openTime, trade);
                currentKlines.put(interval, current);
            } else if (openTime == current.openTime) {
                current.apply(trade);
            } else {
                throw new IllegalArgumentException("成交时间早于当前 KLine 窗口");
            }
        }
        List<KLine> current = currentKlines.values().stream().map(value -> value.toKLine(false)).toList();
        return new KLineAggregationResult(current, closed);
    }

    /** 内部可变 KLine，仅由单交易对消费顺序调用。 */
    private static final class MutableKLine {
        private final String symbol;
        private final KLineInterval interval;
        private final long openTime;
        private BigDecimal open;
        private BigDecimal high;
        private BigDecimal low;
        private BigDecimal close;
        private BigDecimal volume = BigDecimal.ZERO;
        private BigDecimal quoteVolume = BigDecimal.ZERO;
        private long tradeCount;

        private MutableKLine(String symbol, KLineInterval interval, long openTime, MarketTrade trade) {
            this.symbol = symbol;
            this.interval = interval;
            this.openTime = openTime;
            this.open = trade.price();
            this.high = trade.price();
            this.low = trade.price();
            apply(trade);
        }

        private void apply(MarketTrade trade) {
            high = high.max(trade.price());
            low = low.min(trade.price());
            close = trade.price();
            volume = volume.add(trade.quantity());
            quoteVolume = quoteVolume.add(trade.quoteQuantity());
            tradeCount++;
        }

        private KLine toKLine(boolean closed) {
            return new KLine(symbol, interval, openTime, openTime + interval.getDurationMillis() - 1,
                    open, high, low, close, volume, quoteVolume, tradeCount, closed);
        }
    }
}
