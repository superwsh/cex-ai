package com.cex.order.application.service;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.order.infrastructure.kafka.OrderKafkaProducer;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import com.cex.order.infrastructure.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Outbox Relay:将发件箱事件投递到 Kafka
 * 失败指数退避:next_retry_time = now + 2^retry_count 秒,超过最大重试置 FAILED 并告警
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxRelayService {

    public static final int BATCH_SIZE = 100;
    public static final int MAX_RETRY = 10;

    private final OutboxRepository outboxRepository;
    private final OrderKafkaProducer kafkaProducer;
    private final ObjectMapper objectMapper;

    public void relay() {
        List<OrderEventOutboxPO> pending = outboxRepository.findPending(BATCH_SIZE, MAX_RETRY);
        if (pending.isEmpty()) {
            return;
        }
        log.info("Outbox Relay 扫描到 {} 条待发送事件", pending.size());
        for (OrderEventOutboxPO outbox : pending) {
            relayOne(outbox);
        }
    }

    private void relayOne(OrderEventOutboxPO outbox) {
        outbox.setStatus(OrderEventOutboxPO.STATUS_SENDING);
        outbox.setUpdatedAt(LocalDateTime.now());
        outboxRepository.update(outbox);
        try {
            OrderEvent event = objectMapper.readValue(outbox.getPayload(), OrderEvent.class);
            kafkaProducer.send(event);
            outbox.setStatus(OrderEventOutboxPO.STATUS_SUCCESS);
            outbox.setUpdatedAt(LocalDateTime.now());
            outboxRepository.update(outbox);
            log.info("Outbox 事件发送成功: eventId={}, eventType={}, orderId={}",
                    outbox.getEventId(), outbox.getEventType(), outbox.getAggregateId());
        } catch (Exception e) {
            int retryCount = outbox.getRetryCount() + 1;
            outbox.setRetryCount(retryCount);
            if (retryCount >= MAX_RETRY) {
                outbox.setStatus(OrderEventOutboxPO.STATUS_FAILED);
                log.error("Outbox 事件超过最大重试次数,需人工处理: eventId={}, eventType={}, orderId={}",
                        outbox.getEventId(), outbox.getEventType(), outbox.getAggregateId(), e);
            } else {
                outbox.setStatus(OrderEventOutboxPO.STATUS_INIT);
                outbox.setNextRetryTime(LocalDateTime.now().plusSeconds(1L << retryCount));
                log.warn("Outbox 事件发送失败,第 {} 次重试: eventId={}, eventType={}",
                        retryCount, outbox.getEventId(), outbox.getEventType(), e);
            }
            outbox.setUpdatedAt(LocalDateTime.now());
            outboxRepository.update(outbox);
        }
    }
}
