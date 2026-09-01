package com.cex.market.infrastructure.redis;

import com.cex.market.application.service.MarketDataCache;
import com.cex.market.application.service.MarketTradeCacheSnapshot;
import com.cex.market.domain.model.MarketDepthSnapshot;
import com.cex.market.domain.trade.BookTicker;
import com.cex.market.domain.trade.MarketTrade;
import com.cex.market.domain.trade.Ticker24h;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;

/** Redis 行情热点缓存适配器；缓存丢失可通过 Kafka 重放重建。 */
@Component
@RequiredArgsConstructor
public class RedisMarketDataCache implements MarketDataCache {

    private static final String RECENT_TRADES_KEY_PREFIX = "market:trades:";
    private static final String LAST_PRICE_KEY_PREFIX = "market:lastPrice:";
    private static final String TICKER_24H_KEY_PREFIX = "market:ticker:";
    private static final String BOOK_TICKER_KEY_PREFIX = "market:bookTicker:";
    private static final String DEPTH_SNAPSHOT_KEY_PREFIX = "market:depth:";
    private static final String SYMBOLS_KEY = "market:symbols";

    private final RedisTemplate<String, Object> redisTemplate;

    /**
     * 写入一个交易对的成交相关热数据。
     *
     * @param snapshot 聚合成功后的成交行情快照
     */
    @Override
    public void saveTradeSnapshot(MarketTradeCacheSnapshot snapshot) {
        redisTemplate.opsForValue().set(RECENT_TRADES_KEY_PREFIX + snapshot.symbol(), snapshot.recentTrades());
        redisTemplate.opsForValue().set(LAST_PRICE_KEY_PREFIX + snapshot.symbol(), snapshot.lastPrice());
        if (snapshot.ticker24h() != null) {
            redisTemplate.opsForValue().set(TICKER_24H_KEY_PREFIX + snapshot.symbol(), snapshot.ticker24h());
        } else {
            redisTemplate.delete(TICKER_24H_KEY_PREFIX + snapshot.symbol());
        }
        registerSymbol(snapshot.symbol());
    }

    /**
     * 写入一个交易对的最佳买卖报价。
     *
     * @param ticker 最佳买卖报价
     */
    @Override
    public void saveBookTicker(BookTicker ticker) {
        redisTemplate.opsForValue().set(BOOK_TICKER_KEY_PREFIX + ticker.symbol(), ticker);
        registerSymbol(ticker.symbol());
    }

    /**
     * 写入可由 REST 查询的盘口深度快照。
     *
     * @param snapshot 当前盘口快照
     */
    @Override
    public void saveDepthSnapshot(MarketDepthSnapshot snapshot) {
        redisTemplate.opsForValue().set(DEPTH_SNAPSHOT_KEY_PREFIX + snapshot.symbol(), snapshot);
        registerSymbol(snapshot.symbol());
    }

    /**
     * 获取已有热数据的交易对。
     *
     * @return 排序后的交易对列表
     */
    @Override
    public List<String> findSymbols() {
        return redisTemplate.opsForSet().members(SYMBOLS_KEY).stream()
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .sorted(Comparator.naturalOrder())
                .toList();
    }

    /**
     * 获取最近成交并按调用方指定的数量截断。
     *
     * @param symbol 交易对
     * @param limit 最大返回数量
     * @return 最近成交列表
     */
    @Override
    public List<MarketTrade> findRecentTrades(String symbol, int limit) {
        Object value = redisTemplate.opsForValue().get(RECENT_TRADES_KEY_PREFIX + symbol);
        if (!(value instanceof List<?> values)) {
            return List.of();
        }
        return values.stream().filter(MarketTrade.class::isInstance).map(MarketTrade.class::cast)
                .limit(limit).toList();
    }

    /**
     * 获取 24 小时行情统计。
     *
     * @param symbol 交易对
     * @return 24 小时行情，未命中时为空
     */
    @Override
    public Ticker24h findTicker24h(String symbol) {
        Object value = redisTemplate.opsForValue().get(TICKER_24H_KEY_PREFIX + symbol);
        return value instanceof Ticker24h ticker ? ticker : null;
    }

    /**
     * 获取最佳买卖报价。
     *
     * @param symbol 交易对
     * @return 最佳买卖报价，未命中时为空
     */
    @Override
    public BookTicker findBookTicker(String symbol) {
        Object value = redisTemplate.opsForValue().get(BOOK_TICKER_KEY_PREFIX + symbol);
        return value instanceof BookTicker ticker ? ticker : null;
    }

    /**
     * 获取盘口快照，并在读取边界截断档位数。
     *
     * @param symbol 交易对
     * @param limit 买卖两侧最大档位数
     * @return 深度快照，未命中时为空
     */
    @Override
    public MarketDepthSnapshot findDepthSnapshot(String symbol, int limit) {
        Object value = redisTemplate.opsForValue().get(DEPTH_SNAPSHOT_KEY_PREFIX + symbol);
        if (!(value instanceof MarketDepthSnapshot snapshot)) {
            return null;
        }
        return new MarketDepthSnapshot(snapshot.symbol(), snapshot.sequence(),
                snapshot.bids().stream().limit(limit).toList(), snapshot.asks().stream().limit(limit).toList(),
                snapshot.timestamp());
    }

    /**
     * 将出现过实际行情数据的交易对登记到 Redis 集合。
     *
     * @param symbol 交易对
     */
    private void registerSymbol(String symbol) {
        redisTemplate.opsForSet().add(SYMBOLS_KEY, symbol);
    }
}
