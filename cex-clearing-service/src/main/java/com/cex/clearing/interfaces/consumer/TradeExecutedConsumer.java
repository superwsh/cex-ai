package com.cex.clearing.interfaces.consumer;

import com.cex.clearing.application.service.SettlementTaskApplicationService;
import com.cex.clearing.application.service.TradeSettlementApplicationService;
import com.cex.clearing.application.observability.ClearingAlertService;
import com.cex.clearing.application.observability.ClearingMetrics;
import com.cex.clearing.domain.settlement.SettlementException;
import com.cex.clearing.domain.settlement.SettlementStatus;
import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.TradeEvent;
import com.cex.clearing.infrastructure.kafka.TradeEventRetryPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Kafka 成交消费者：建档后进入结算；已建档任务的失败重试由数据库恢复任务统一调度。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TradeExecutedConsumer {
    private final SettlementTaskApplicationService settlementTaskApplicationService;
    private final TradeSettlementApplicationService tradeSettlementApplicationService;
    private final TradeEventRetryPublisher retryPublisher;
    private final ClearingMetrics clearingMetrics;
    private final ClearingAlertService clearingAlertService;

    @KafkaListener(topics = TopicConstants.TOPIC_TRADE_EVENT, groupId = "cex-clearing")
    public void onTradeExecuted(TradeEvent event) {
        consume(event, 0);
    }

    /** 消费重试主题，超过最大次数后转入 DLQ。 */
    @KafkaListener(topics = TopicConstants.TOPIC_TRADE_EVENT_RETRY, groupId = "cex-clearing")
    public void onTradeRetry(TradeEvent event,
                             @Header(name = TradeEventRetryPublisher.RETRY_COUNT_HEADER, required = false)
                             Integer retryCount) {
        consume(event, retryCount == null ? 0 : retryCount);
    }

    private void consume(TradeEvent event, int retryCount) {
        long startedNanos = System.nanoTime();
        clearingMetrics.recordConsumerLag(event == null ? null : event.getTimestamp());
        try {
            SettlementTaskApplicationService.RegistrationResult result = settlementTaskApplicationService.register(event);
            if (shouldSettle(result)) {
                boolean settled = tradeSettlementApplicationService.settle(event);
                if (settled) {
                    clearingMetrics.recordSettlementSuccess(startedNanos);
                } else {
                    clearingMetrics.recordDuplicateTrade();
                }
            } else {
                clearingMetrics.recordDuplicateTrade();
            }
            log.info("成交结算任务已处理: tradeId={}, eventId={}, created={}, existingStatus={}",
                    event.getTradeId(), event.getEventId(), result.created(), result.existingStatus());
        } catch (SettlementException exception) {
            clearingMetrics.recordSettlementFailure(exception.getErrorCode());
            handleSettlementException(event, retryCount, exception);
        } catch (Exception exception) {
            clearingMetrics.recordSettlementFailure("UNKNOWN_SETTLEMENT_ERROR");
            handleSettlementException(event, retryCount,
                    new SettlementException("UNKNOWN_SETTLEMENT_ERROR", "结算任务处理异常", true, exception));
        }
    }

    private void handleSettlementException(TradeEvent event, int retryCount, SettlementException exception) {
        if (!exception.isRetryable()) {
            settlementTaskApplicationService.markManualReview(resolveTradeId(event), exception.getErrorCode(), exception.getMessage());
            clearingMetrics.recordManualReview();
            clearingAlertService.alert("SETTLEMENT_MANUAL_REVIEW", resolveTradeId(event), exception.getErrorCode());
            retryPublisher.publishDlq(event, exception.getErrorCode());
            log.error("成交事件进入 DLQ: tradeId={}, errorCode={}", resolveTradeId(event), exception.getErrorCode());
            return;
        }
        SettlementTaskApplicationService.RetryScheduleResult result = settlementTaskApplicationService
                .scheduleRetry(resolveTradeId(event), exception.getErrorCode(), exception.getMessage());
        if (result.scheduled()) {
            if (result.manualReview()) {
                clearingMetrics.recordManualReview();
                clearingAlertService.alert("SETTLEMENT_RETRY_EXHAUSTED", resolveTradeId(event), exception.getErrorCode());
                retryPublisher.publishDlq(event, exception.getErrorCode());
                log.error("结算任务重试耗尽，进入人工复核和 DLQ: tradeId={}, retryCount={}, errorCode={}",
                        resolveTradeId(event), result.retryCount(), exception.getErrorCode());
                return;
            }
            log.warn("结算任务已持久化为 RETRY，等待恢复任务执行: tradeId={}, retryCount={}, errorCode={}",
                    resolveTradeId(event), result.retryCount(), exception.getErrorCode());
            clearingMetrics.recordSettlementRetry();
            return;
        }
        publishKafkaRetryFallback(event, retryCount, exception);
    }

    /** 仅当结算任务尚未落库时使用 Kafka Retry，已建档任务由数据库恢复任务统一调度。 */
    private void publishKafkaRetryFallback(TradeEvent event, int retryCount, SettlementException exception) {
        if (retryCount >= SettlementTaskApplicationService.MAX_RETRY) {
            settlementTaskApplicationService.markManualReview(resolveTradeId(event), exception.getErrorCode(), exception.getMessage());
            clearingMetrics.recordManualReview();
            clearingAlertService.alert("KAFKA_RETRY_EXHAUSTED", resolveTradeId(event), exception.getErrorCode());
            retryPublisher.publishDlq(event, exception.getErrorCode());
            log.error("成交事件重试耗尽，进入 DLQ: tradeId={}, retryCount={}, errorCode={}",
                    resolveTradeId(event), retryCount, exception.getErrorCode());
            return;
        }
        retryPublisher.publishRetry(event, retryCount + 1, exception.getErrorCode());
        clearingMetrics.recordSettlementRetry();
        log.warn("成交事件进入 Retry Topic: tradeId={}, retryCount={}, errorCode={}",
                resolveTradeId(event), retryCount + 1, exception.getErrorCode());
    }

    /** 判断新建或可重试任务是否应进入 Phase 5 事务结算。 */
    private boolean shouldSettle(SettlementTaskApplicationService.RegistrationResult result) {
        return result.created() || SettlementStatus.INIT.name().equals(result.existingStatus())
                || SettlementStatus.RETRY.name().equals(result.existingStatus());
    }

    private String resolveTradeId(TradeEvent event) {
        return event == null ? null : event.getTradeId();
    }
}
