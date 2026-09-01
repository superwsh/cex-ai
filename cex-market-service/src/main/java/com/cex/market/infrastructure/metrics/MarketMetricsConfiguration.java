package com.cex.market.infrastructure.metrics;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;

/** 初始化市场服务基础监控指标。 */
@Configuration
@RequiredArgsConstructor
public class MarketMetricsConfiguration {

    private final MarketMetrics marketMetrics;

    /** 注册基础 Gauge，确保无连接时也能被 Prometheus 抓取。 */
    @PostConstruct
    public void initialize() {
        marketMetrics.initialize();
    }
}
