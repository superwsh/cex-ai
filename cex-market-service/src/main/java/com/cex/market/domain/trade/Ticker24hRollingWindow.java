package com.cex.market.domain.trade;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/** 基于分钟桶维护的单交易对 24 小时滚动行情窗口。 */
public final class Ticker24hRollingWindow {

    private static final long MINUTE_MILLIS = 60_000L;
    private static final long WINDOW_MILLIS = 24 * 60 * MINUTE_MILLIS;
    private static final int PERCENT_SCALE = 8;

    private final String symbol;
    private final NavigableMap<Long, TickerMinuteBucket> buckets = new TreeMap<>();
    private BigDecimal lastPrice;

    /**
     * 创建指定交易对的滚动窗口。
     *
     * @param symbol 交易对
     */
    public Ticker24hRollingWindow(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        this.symbol = symbol;
    }

    /**
     * 加入一笔成交并计算当前 24 小时 Ticker。
     *
     * @param trade 新成交
     * @param now 当前计算时间（毫秒时间戳）
     * @return 当前 24 小时行情；历史成交不在窗口时为空
     */
    public Optional<Ticker24h> add(MarketTrade trade, long now) {
        if (!symbol.equals(trade.symbol()) || now <= 0) {
            throw new IllegalArgumentException("成交交易对或计算时间非法");
        }
        long cutoff = now - WINDOW_MILLIS;
        if (trade.timestamp() >= cutoff) {
            long minute = minuteOf(trade.timestamp());
            buckets.computeIfAbsent(minute, TickerMinuteBucket::new).add(trade);
            lastPrice = trade.price();
        }
        evictBefore(cutoff);
        if (buckets.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(aggregate(now));
    }

    /**
     * 获取当前窗口的最后成交价。
     *
     * @return 最新成交价；尚无窗口内成交时为空
     */
    public BigDecimal lastPrice() {
        return lastPrice;
    }

    /**
     * 淘汰过期分钟桶并返回当前窗口的 Ticker，供定时缓存刷新使用。
     *
     * @param now 当前计算时间（毫秒时间戳）
     * @return 当前窗口行情；窗口内无成交时为空
     */
    public Optional<Ticker24h> current(long now) {
        if (now <= 0) {
            throw new IllegalArgumentException("计算时间必须大于零");
        }
        evictBefore(now - WINDOW_MILLIS);
        return buckets.isEmpty() ? Optional.empty() : Optional.of(aggregate(now));
    }

    private Ticker24h aggregate(long now) {
        TickerMinuteBucket first = buckets.firstEntry().getValue();
        BigDecimal high = null;
        BigDecimal low = null;
        BigDecimal volume = BigDecimal.ZERO;
        BigDecimal quoteVolume = BigDecimal.ZERO;
        long tradeCount = 0;
        TickerMinuteBucket last = null;
        for (TickerMinuteBucket bucket : buckets.values()) {
            high = high == null ? bucket.high() : high.max(bucket.high());
            low = low == null ? bucket.low() : low.min(bucket.low());
            volume = volume.add(bucket.volume());
            quoteVolume = quoteVolume.add(bucket.quoteVolume());
            tradeCount += bucket.tradeCount();
            last = bucket;
        }
        BigDecimal close = lastPrice == null ? last.close() : lastPrice;
        BigDecimal change = close.subtract(first.open());
        BigDecimal percent = change.multiply(BigDecimal.valueOf(100))
                .divide(first.open(), PERCENT_SCALE, RoundingMode.HALF_UP);
        return new Ticker24h(symbol, close, first.open(), high, low, change, percent, volume, quoteVolume,
                tradeCount, first.minute(), now);
    }

    private void evictBefore(long cutoff) {
        long cutoffMinute = minuteOf(cutoff);
        buckets.headMap(cutoffMinute, false).clear();
    }

    private long minuteOf(long timestamp) {
        return timestamp / MINUTE_MILLIS * MINUTE_MILLIS;
    }
}
