package com.cex.market.interfaces.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 市场公开 REST 接口固定窗口限流配置。 */
@ConfigurationProperties(prefix = "cex.market.rate-limit")
public class MarketRateLimitProperties {

    /** 每个客户端在一个窗口内允许的最大请求数。 */
    private int requestsPerWindow = 120;

    /** 限流窗口长度，单位为秒。 */
    private int windowSeconds = 60;

    /**
     * 获取每窗口最大请求数。
     *
     * @return 请求上限
     */
    public int getRequestsPerWindow() {
        return requestsPerWindow;
    }

    /**
     * 设置每窗口最大请求数。
     *
     * @param requestsPerWindow 请求上限
     */
    public void setRequestsPerWindow(int requestsPerWindow) {
        this.requestsPerWindow = requestsPerWindow;
    }

    /**
     * 获取限流窗口长度。
     *
     * @return 窗口秒数
     */
    public int getWindowSeconds() {
        return windowSeconds;
    }

    /**
     * 设置限流窗口长度。
     *
     * @param windowSeconds 窗口秒数
     */
    public void setWindowSeconds(int windowSeconds) {
        this.windowSeconds = windowSeconds;
    }
}
