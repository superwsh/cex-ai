package com.cex.market.interfaces.consumer;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.TradeEvent;
import com.cex.market.application.service.MarketTradeApplicationService;
import com.cex.market.application.service.MarketTradeProcessingResult;
import com.cex.market.application.service.KLineApplicationService;
import com.cex.market.application.service.MarketDataPublisher;
import com.cex.market.infrastructure.metrics.MarketMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/** 消费撮合成交事件并维护成交、Ticker 和 Redis 热缓存。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MarketTradeConsumer {

    private final MarketTradeApplicationService marketTradeApplicationService;
    private final KLineApplicationService kLineApplicationService;
    private final MarketDataPublisher marketDataPublisher;
    private final MarketMetrics marketMetrics;

    /**
     * 以 symbol 分区顺序消费成交事件。处理失败时抛出异常，由 Kafka 依配置重试该记录。
     *
     * @param event 撮合引擎发布的成交事件
     * @param partition Kafka 分区
     * @param offset Kafka 位点
     */
    @KafkaListener(topics = TopicConstants.TOPIC_TRADE_EVENT, groupId = "cex-market")
    public void onTrade(TradeEvent event,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {
        MarketTradeProcessingResult result = marketTradeApplicationService.process(event);
        var currentKlines = kLineApplicationService.process(result.trade());
        if (!result.duplicate()) {
            marketDataPublisher.publishTrade(result.trade());
            if (result.snapshot().ticker24h() != null) {
                marketDataPublisher.publishTicker(result.snapshot().ticker24h());
            }
        }
        currentKlines.forEach(marketDataPublisher::publishKLine);
        marketMetrics.recordEventDelay("trade", event.getTimestamp());
        log.info("行情成交已处理: tradeId={}, eventId={}, symbol={}, duplicate={}, partition={}, offset={}",
                event.getTradeId(), event.getEventId(), event.getSymbol(), result.duplicate(), partition, offset);
    }
}
