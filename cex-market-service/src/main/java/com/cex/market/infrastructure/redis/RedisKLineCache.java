package com.cex.market.infrastructure.redis;

import com.cex.market.application.service.KLineCache;
import com.cex.market.domain.kline.KLine;
import com.cex.market.domain.kline.KLineInterval;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

/** Redis 当前 KLine 缓存适配器。 */
@Component
@RequiredArgsConstructor
public class RedisKLineCache implements KLineCache {

    private static final String KEY_PREFIX = "market:kline:";

    private final RedisTemplate<String, Object> redisTemplate;

    /**
     * 写入当前未收线 KLine。
     *
     * @param kLine 当前 KLine
     */
    @Override
    public void saveCurrent(KLine kLine) {
        if (kLine.closed()) {
            throw new IllegalArgumentException("当前 KLine 缓存不能写入已收线数据");
        }
        redisTemplate.opsForValue().set(key(kLine.symbol(), kLine.interval()), kLine);
    }

    /**
     * 读取当前 KLine。
     *
     * @param symbol 交易对
     * @param interval 周期
     * @return 当前 KLine；缓存不存在或类型不匹配时为空
     */
    @Override
    public KLine getCurrent(String symbol, KLineInterval interval) {
        Object value = redisTemplate.opsForValue().get(key(symbol, interval));
        return value instanceof KLine kLine ? kLine : null;
    }

    private String key(String symbol, KLineInterval interval) {
        return KEY_PREFIX + symbol + ":" + interval.getCode();
    }
}
