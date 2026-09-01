package com.cex.market.interfaces.config;

import com.cex.common.core.exception.BizException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.time.Duration;

/** 使用 Redis 固定窗口计数器保护市场只读接口。 */
@Component
@RequiredArgsConstructor
public class MarketRateLimitInterceptor implements HandlerInterceptor {

    private static final String KEY_PREFIX = "market:rate-limit:";

    private final StringRedisTemplate stringRedisTemplate;
    private final MarketRateLimitProperties properties;

    /**
     * 在进入控制器前按客户端 IP 限制市场查询请求。
     *
     * @param request 当前 HTTP 请求
     * @param response 当前 HTTP 响应
     * @param handler Spring 请求处理器
     * @return 请求是否允许继续
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        validateProperties();
        long window = System.currentTimeMillis() / (properties.getWindowSeconds() * 1_000L);
        String key = KEY_PREFIX + request.getRemoteAddr() + ":" + window;
        Boolean created = stringRedisTemplate.opsForValue().setIfAbsent(key, "1",
                Duration.ofSeconds(properties.getWindowSeconds()));
        if (Boolean.TRUE.equals(created)) {
            return true;
        }
        Long requestCount = stringRedisTemplate.opsForValue().increment(key);
        if (requestCount != null && requestCount <= properties.getRequestsPerWindow()) {
            return true;
        }
        throw new BizException(429, "请求过于频繁，请稍后重试");
    }

    /**
     * 校验限流配置，避免错误配置使所有接口长期不可用。
     */
    private void validateProperties() {
        if (properties.getRequestsPerWindow() <= 0 || properties.getWindowSeconds() <= 0) {
            throw new IllegalStateException("市场接口限流配置必须大于零");
        }
    }
}
