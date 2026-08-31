package com.cex.clearing.application.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.clearing.infrastructure.persistence.entity.SettlementEventOutboxPO;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementEventOutboxMapper;
import com.cex.clearing.application.observability.ClearingAlertService;
import com.cex.clearing.application.observability.ClearingMetrics;
import com.cex.common.kafka.event.TradeSettledEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** 将已提交的结算 Outbox 可靠投递到 Kafka。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SettlementOutboxRelayService {

    private static final int MAX_RETRY = 10;
    private static final int BATCH_SIZE = 100;
    private static final int PUBLISH_LEASE_SECONDS = 30;
    private static final int MAX_ERROR_MESSAGE_LENGTH = 512;
    private final SettlementEventOutboxMapper outboxMapper;
    private final KafkaTemplate<String, TradeSettledEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ClearingMetrics clearingMetrics;
    private final ClearingAlertService clearingAlertService;

    /** 扫描并投递最多一百条到期事件。SENDING 状态仅在发送租约到期后才会被恢复。 */
    public void relay() {
        List<SettlementEventOutboxPO> records = outboxMapper.selectList(new LambdaQueryWrapper<SettlementEventOutboxPO>()
                .in(SettlementEventOutboxPO::getStatus, "NEW", "RETRY", "SENDING")
                .le(SettlementEventOutboxPO::getNextRetryTime, LocalDateTime.now())
                .lt(SettlementEventOutboxPO::getRetryCount, MAX_RETRY)
                .orderByAsc(SettlementEventOutboxPO::getId).last("LIMIT " + BATCH_SIZE));
        for (SettlementEventOutboxPO record : records) {
            relayOne(record);
        }
        refreshOutboxGauges();
    }

    /** 原子认领单条 Outbox 后发送，避免多实例并发扫描时由非持有者篡改最终状态。 */
    private void relayOne(SettlementEventOutboxPO record) {
        LocalDateTime now = LocalDateTime.now();
        String processingToken = UUID.randomUUID().toString();
        if (outboxMapper.claimForPublish(record.getId(), processingToken,
                now.plusSeconds(PUBLISH_LEASE_SECONDS), now, MAX_RETRY) != 1) {
            return;
        }
        try {
            TradeSettledEvent event = objectMapper.readValue(record.getPayload(), TradeSettledEvent.class);
            kafkaTemplate.send(record.getTopic(), event.getSymbol(), event).get(5, TimeUnit.SECONDS);
            if (outboxMapper.markPublished(record.getId(), processingToken, LocalDateTime.now()) != 1) {
                log.warn("结算 Outbox 发布完成但发送租约已失效: eventId={}", record.getEventId());
            }
        } catch (Exception exception) {
            int retries = record.getRetryCount() + 1;
            String status = retries >= MAX_RETRY ? "FAILED" : "RETRY";
            LocalDateTime retryTime = LocalDateTime.now()
                    .plusSeconds(Math.min(300, 1L << Math.min(retries, 8)));
            outboxMapper.markRetry(record.getId(), processingToken, status, retries, retryTime,
                    errorMessage(exception), LocalDateTime.now());
            if ("FAILED".equals(status)) {
                clearingAlertService.alert("OUTBOX_FAILED", record.getAggregateId(), "KAFKA_PUBLISH_FAILED");
            }
            log.error("结算 Outbox 投递失败: eventId={}, retry={}", record.getEventId(), retries, exception);
        }
    }

    /** 保留可审计的异常摘要，避免超长堆栈写入数据库。 */
    private String errorMessage(Exception exception) {
        String message = exception.getClass().getSimpleName() + ": " + exception.getMessage();
        return message.length() <= MAX_ERROR_MESSAGE_LENGTH
                ? message : message.substring(0, MAX_ERROR_MESSAGE_LENGTH);
    }

    /** 从数据库刷新真实待发送和失败数量，避免 gauge 只反映单个批次。 */
    private void refreshOutboxGauges() {
        Long pending = outboxMapper.selectCount(new LambdaQueryWrapper<SettlementEventOutboxPO>()
                .in(SettlementEventOutboxPO::getStatus, "NEW", "RETRY", "SENDING"));
        Long failed = outboxMapper.selectCount(new LambdaQueryWrapper<SettlementEventOutboxPO>()
                .eq(SettlementEventOutboxPO::getStatus, "FAILED"));
        clearingMetrics.setOutboxPending(pending == null ? 0 : pending);
        clearingMetrics.setOutboxFailed(failed == null ? 0 : failed);
    }
}
