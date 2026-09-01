package com.cex.market.interfaces.consumer;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.market.OrderBookDeltaEvent;
import com.cex.market.application.service.MarketOrderBookApplicationService;
import com.cex.market.application.service.MarketOrderBookProcessingResult;
import com.cex.market.infrastructure.metrics.MarketMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/** 消费撮合订单簿增量，维护可恢复的行情订单簿。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MarketOrderBookDeltaConsumer {

    private final MarketOrderBookApplicationService marketOrderBookApplicationService;
    private final MarketMetrics marketMetrics;

    /**
     * 以 Kafka 分区顺序消费盘口增量；快照持久化或重放失败会抛出，使当前记录不提交。
     *
     * @param event 盘口增量事件
     * @param partition Kafka 分区
     * @param offset Kafka 位点
     */
    @KafkaListener(topics = TopicConstants.TOPIC_ORDER_BOOK_DELTA_EVENT, groupId = "cex-market")
    public void onDelta(OrderBookDeltaEvent event, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {
        MarketOrderBookProcessingResult result = marketOrderBookApplicationService.process(event, partition, offset);
        marketMetrics.recordEventDelay("order_book_delta", event.getEventTime());
        log.info("行情盘口增量已处理: eventId={}, symbol={}, sequence={}, duplicate={}, recovered={}, partition={}, offset={}",
                event.getEventId(), event.getSymbol(), event.getSequence(),
                result.applyResult().name().equals("IGNORED_DUPLICATE"), result.recovered(), partition, offset);
    }
}
