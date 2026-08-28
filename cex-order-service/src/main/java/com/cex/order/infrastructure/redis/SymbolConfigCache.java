package com.cex.order.infrastructure.redis;

import com.cex.order.domain.service.SymbolConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 交易对配置 Redis 缓存(cache-aside,10 分钟 TTL)
 * 注意:订单最终状态不依赖缓存,缓存仅做配置加速
 */
@Component
@RequiredArgsConstructor
public class SymbolConfigCache {

    private static final String KEY_PREFIX = "symbol:config:";
    private static final Duration TTL = Duration.ofMinutes(10);

    private final RedisTemplate<String, Object> redisTemplate;

    public SymbolConfig get(String symbol) {
        Object value = redisTemplate.opsForValue().get(KEY_PREFIX + symbol);
        return value instanceof SymbolConfig config ? config : null;
    }

    public void put(String symbol, SymbolConfig config) {
        redisTemplate.opsForValue().set(KEY_PREFIX + symbol, config, TTL);
    }
}
