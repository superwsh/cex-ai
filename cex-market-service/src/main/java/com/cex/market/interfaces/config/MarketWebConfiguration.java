package com.cex.market.interfaces.config;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** 注册市场公开 REST 接口的 Web 基础设施。 */
@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(MarketRateLimitProperties.class)
public class MarketWebConfiguration implements WebMvcConfigurer {

    private final MarketRateLimitInterceptor marketRateLimitInterceptor;

    /**
     * 为全部市场 REST 查询接口注册限流拦截器。
     *
     * @param registry Spring MVC 拦截器注册表
     */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(marketRateLimitInterceptor).addPathPatterns("/api/v1/market/**");
    }
}
