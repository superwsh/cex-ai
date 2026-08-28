package com.cex.order.infrastructure.kafka;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 订单事件 Kafka 生产者
 * key=symbol,保证同交易对事件进入同一分区保持顺序
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderKafkaProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void send(OrderEvent event) {
        try {
            kafkaTemplate.send(TopicConstants.TOPIC_ORDER_EVENT, event.getSymbol(), event)
                    .get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka 发送失败: topic=" + TopicConstants.TOPIC_ORDER_EVENT
                    + ", symbol=" + event.getSymbol() + ", orderId=" + event.getOrderId(), e);
        }
    }
}
