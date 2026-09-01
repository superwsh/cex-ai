package com.cex.market.application.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 定时推进滚动 Ticker 窗口，避免无新成交时缓存停留在过期统计。 */
@Component
@RequiredArgsConstructor
public class MarketTickerRefreshScheduler {

    private final MarketTradeApplicationService marketTradeApplicationService;
    private final MarketDataPublisher marketDataPublisher;

    /** 每分钟刷新已活跃交易对的 24 小时 Ticker 缓存。 */
    @Scheduled(fixedDelayString = "${cex.market.ticker-refresh-interval-ms:60000}")
    public void refresh() {
        marketTradeApplicationService.refreshTickerCaches().forEach(marketDataPublisher::publishTicker);
    }
}
