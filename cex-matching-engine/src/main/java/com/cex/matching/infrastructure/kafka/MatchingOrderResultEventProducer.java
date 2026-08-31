package com.cex.matching.infrastructure.kafka;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.OrderResultEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/** 同步投递订单状态结果，失败时由上游命令重试。 */
@Component
@RequiredArgsConstructor
public class MatchingOrderResultEventProducer {
    private final KafkaTemplate<String, OrderResultEvent> kafkaTemplate;

    /**
     * 以交易对为键发送订单状态结果。
     *
     * @param event 已完成映射的订单结果
     */
    public void send(OrderResultEvent event) {
        try {
            kafkaTemplate.send(TopicConstants.TOPIC_ORDER_RESULT_EVENT, event.getSymbol(), event)
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception exception) {
            throw new IllegalStateException("订单结果事件发送失败: eventId=" + event.getEventId(), exception);
        }
    }
}
