package com.cex.clearing.application.service;

import com.cex.clearing.domain.settlement.SettlementException;
import com.cex.clearing.application.observability.ClearingAlertService;
import com.cex.clearing.application.observability.ClearingMetrics;
import com.cex.clearing.infrastructure.kafka.TradeEventRetryPublisher;
import com.cex.clearing.infrastructure.persistence.entity.SettlementTaskPO;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementTaskMapper;
import com.cex.common.kafka.event.TradeEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

/** 从 settlement_task 快照恢复超时或可重试的结算，避免依赖 Kafka Offset 作为恢复依据。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SettlementRecoveryApplicationService {

    private static final int PROCESSING_TIMEOUT_SECONDS = 300;
    private static final int INIT_TIMEOUT_SECONDS = 60;
    private static final int RETRY_BATCH_SIZE = 100;

    private final SettlementTaskMapper settlementTaskMapper;
    private final SettlementTaskApplicationService settlementTaskApplicationService;
    private final TradeSettlementApplicationService tradeSettlementApplicationService;
    private final TradeEventValidator tradeEventValidator;
    private final TradeEventRetryPublisher retryPublisher;
    private final ClearingMetrics clearingMetrics;
    private final ClearingAlertService clearingAlertService;

    /** 恢复遗留任务并执行到期重试；每笔结算仍由独立的结算事务保证原子性。 */
    public void recoverAndRetry() {
        LocalDateTime now = LocalDateTime.now();
        int recoveredProcessing = settlementTaskMapper.recoverExpiredProcessing(
                now.minusSeconds(PROCESSING_TIMEOUT_SECONDS), SettlementTaskApplicationService.MAX_RETRY, now);
        int recoveredInit = settlementTaskMapper.recoverExpiredInit(now.minusSeconds(INIT_TIMEOUT_SECONDS), now);
        if (recoveredProcessing > 0 || recoveredInit > 0) {
            log.warn("结算任务恢复扫描完成: processingRecovered={}, initRecovered={}", recoveredProcessing, recoveredInit);
        }
        List<SettlementTaskPO> tasks = settlementTaskMapper.selectDueRetryTasks(LocalDateTime.now(), RETRY_BATCH_SIZE);
        for (SettlementTaskPO task : tasks) {
            retryOne(task);
        }
    }

    /** 使用持久化成交快照重建事件，避免重启后因原始 Kafka 消息不可得而遗失结算。 */
    private void retryOne(SettlementTaskPO task) {
        TradeEvent event = toTradeEvent(task);
        long startedNanos = System.nanoTime();
        try {
            tradeEventValidator.validate(event);
            boolean settled = tradeSettlementApplicationService.settle(event);
            if (settled) {
                clearingMetrics.recordSettlementSuccess(startedNanos);
                log.info("结算恢复重试成功: tradeId={}, retryCount={}", task.getTradeId(), task.getRetryCount());
            } else {
                clearingMetrics.recordDuplicateTrade();
            }
        } catch (SettlementException exception) {
            clearingMetrics.recordSettlementFailure(exception.getErrorCode());
            handleFailure(task, event, exception);
        } catch (Exception exception) {
            clearingMetrics.recordSettlementFailure("UNKNOWN_SETTLEMENT_ERROR");
            handleFailure(task, event, new SettlementException("UNKNOWN_SETTLEMENT_ERROR", "结算恢复执行异常", true, exception));
        }
    }

    /** 将失败写回持久化任务；永久失败和重试耗尽必须同时进入人工复核与 DLQ。 */
    private void handleFailure(SettlementTaskPO task, TradeEvent event, SettlementException exception) {
        if (!exception.isRetryable()) {
            settlementTaskApplicationService.markManualReview(task.getTradeId(), exception.getErrorCode(), exception.getMessage());
            clearingMetrics.recordManualReview();
            clearingAlertService.alert("RECOVERY_MANUAL_REVIEW", task.getTradeId(), exception.getErrorCode());
            retryPublisher.publishDlq(event, exception.getErrorCode());
            log.error("结算恢复发现永久失败，进入人工复核和 DLQ: tradeId={}, errorCode={}",
                    task.getTradeId(), exception.getErrorCode());
            return;
        }
        SettlementTaskApplicationService.RetryScheduleResult result = settlementTaskApplicationService
                .scheduleRetry(task.getTradeId(), exception.getErrorCode(), exception.getMessage());
        if (result.manualReview()) {
            clearingMetrics.recordManualReview();
            clearingAlertService.alert("RECOVERY_RETRY_EXHAUSTED", task.getTradeId(), exception.getErrorCode());
            retryPublisher.publishDlq(event, exception.getErrorCode());
            log.error("结算恢复重试耗尽，进入人工复核和 DLQ: tradeId={}, retryCount={}, errorCode={}",
                    task.getTradeId(), result.retryCount(), exception.getErrorCode());
        } else if (result.scheduled()) {
            clearingMetrics.recordSettlementRetry();
        }
    }

    /** 从持久化任务中恢复完整成交事实。 */
    private TradeEvent toTradeEvent(SettlementTaskPO task) {
        long timestamp = task.getTradeTime() != null ? task.getTradeTime()
                : task.getCreatedAt().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        return TradeEvent.builder().tradeId(task.getTradeId()).eventId(task.getEventId())
                .sequence(task.getMatchSequence()).symbol(task.getSymbol()).buyOrderId(task.getBuyOrderId())
                .sellOrderId(task.getSellOrderId()).buyerUserId(task.getBuyerUserId()).sellerUserId(task.getSellerUserId())
                .baseAsset(task.getBaseAsset()).quoteAsset(task.getQuoteAsset()).price(task.getPrice())
                .quantity(task.getQuantity()).amount(task.getQuoteAmount()).buyerFee(task.getBuyerFee())
                .buyerFeeAsset(task.getBuyerFeeAsset()).sellerFee(task.getSellerFee())
                .sellerFeeAsset(task.getSellerFeeAsset()).timestamp(timestamp).build();
    }
}
