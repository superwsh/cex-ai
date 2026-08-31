package com.cex.clearing.application.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.clearing.domain.settlement.SettlementException;
import com.cex.clearing.domain.settlement.SettlementStatus;
import com.cex.clearing.infrastructure.persistence.entity.SettlementTaskPO;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementTaskMapper;
import com.cex.common.kafka.event.TradeEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Phase 3 结算任务建档服务。
 * 仅持久化经过校验的成交事实和幂等状态，不执行任何余额变更。
 */
@Service
@RequiredArgsConstructor
public class SettlementTaskApplicationService {

    public static final int MAX_RETRY = 5;

    private final SettlementTaskMapper settlementTaskMapper;
    private final TradeEventValidator tradeEventValidator;

    /** 创建结算任务；同一成交重复投递时返回已有任务。 */
    @Transactional
    public RegistrationResult register(TradeEvent event) {
        tradeEventValidator.validate(event);
        SettlementTaskPO existing = findByTradeId(event.getTradeId());
        if (existing != null) {
            return RegistrationResult.duplicate(existing.getStatus());
        }
        try {
            settlementTaskMapper.insert(toTask(event));
            return RegistrationResult.newTask();
        } catch (DuplicateKeyException exception) {
            SettlementTaskPO concurrentTask = findByTradeId(event.getTradeId());
            if (concurrentTask != null) {
                return RegistrationResult.duplicate(concurrentTask.getStatus());
            }
            throw new SettlementException("DUPLICATE_TRADE", "成交幂等键冲突", true, exception);
        } catch (DataAccessException exception) {
            throw new SettlementException("DATABASE_TIMEOUT", "结算任务落库失败", true, exception);
        }
    }

    /** 将可定位的永久失败任务标为人工复核。 */
    @Transactional
    public void markManualReview(String tradeId, String errorCode, String errorMessage) {
        if (tradeId == null || tradeId.isBlank()) {
            return;
        }
        SettlementTaskPO task = findByTradeId(tradeId);
        if (task == null || SettlementStatus.SUCCESS.name().equals(task.getStatus())) {
            return;
        }
        task.setStatus(SettlementStatus.MANUAL_REVIEW.name());
        task.setErrorCode(errorCode);
        task.setErrorMessage(errorCode + ": " + errorMessage);
        task.setUpdatedAt(LocalDateTime.now());
        settlementTaskMapper.updateById(task);
    }

    /** 持久化一次可恢复失败，并根据重试上限决定是否需要人工复核。 */
    @Transactional
    public RetryScheduleResult scheduleRetry(String tradeId, String errorCode, String errorMessage) {
        if (tradeId == null || tradeId.isBlank()) {
            return RetryScheduleResult.notScheduled();
        }
        SettlementTaskPO existing = findByTradeId(tradeId);
        if (existing == null || SettlementStatus.SUCCESS.name().equals(existing.getStatus())
                || SettlementStatus.MANUAL_REVIEW.name().equals(existing.getStatus())) {
            return RetryScheduleResult.notScheduled();
        }
        int nextRetryCount = safeRetryCount(existing) + 1;
        LocalDateTime now = LocalDateTime.now();
        int affectedRows = settlementTaskMapper.scheduleRetry(tradeId, MAX_RETRY,
                now.plusSeconds(backoffSeconds(nextRetryCount)), errorCode, formatError(errorCode, errorMessage), now);
        if (affectedRows != 1) {
            return RetryScheduleResult.notScheduled();
        }
        return new RetryScheduleResult(true, nextRetryCount, nextRetryCount >= MAX_RETRY);
    }

    private SettlementTaskPO findByTradeId(String tradeId) {
        return settlementTaskMapper.selectOne(new LambdaQueryWrapper<SettlementTaskPO>()
                .eq(SettlementTaskPO::getTradeId, tradeId));
    }

    private SettlementTaskPO toTask(TradeEvent event) {
        LocalDateTime now = LocalDateTime.now();
        SettlementTaskPO task = new SettlementTaskPO();
        task.setTradeId(event.getTradeId());
        task.setEventId(event.getEventId());
        task.setSymbol(event.getSymbol());
        task.setBuyOrderId(event.getBuyOrderId());
        task.setSellOrderId(event.getSellOrderId());
        task.setBuyerUserId(event.getBuyerUserId());
        task.setSellerUserId(event.getSellerUserId());
        task.setBaseAsset(event.getBaseAsset());
        task.setQuoteAsset(event.getQuoteAsset());
        task.setPrice(event.getPrice());
        task.setQuantity(event.getQuantity());
        task.setQuoteAmount(event.getAmount());
        task.setBuyerFee(event.getBuyerFee());
        task.setBuyerFeeAsset(event.getBuyerFeeAsset());
        task.setSellerFee(event.getSellerFee());
        task.setSellerFeeAsset(event.getSellerFeeAsset());
        task.setMatchSequence(event.getSequence());
        task.setTradeTime(event.getTimestamp());
        task.setStatus(SettlementStatus.INIT.name());
        task.setRetryCount(0);
        task.setCreatedAt(now);
        task.setUpdatedAt(now);
        return task;
    }

    /** Consumer 使用的建档结果。 */
    public record RegistrationResult(boolean created, String existingStatus) {
        static RegistrationResult newTask() {
            return new RegistrationResult(true, null);
        }

        static RegistrationResult duplicate(String status) {
            return new RegistrationResult(false, status);
        }
    }

    /** 记录一次持久化重试调度的结果。 */
    public record RetryScheduleResult(boolean scheduled, int retryCount, boolean manualReview) {
        static RetryScheduleResult notScheduled() {
            return new RetryScheduleResult(false, 0, false);
        }
    }

    /** 计算受限指数退避，防止故障期间高频打满数据库。 */
    private long backoffSeconds(int retryCount) {
        return Math.min(300, 1L << Math.min(retryCount, 8));
    }

    /** 统一资金任务错误格式，防止数据库错误列被无界异常内容撑爆。 */
    private String formatError(String errorCode, String errorMessage) {
        String message = errorCode + ": " + (errorMessage == null ? "" : errorMessage);
        return message.length() <= 512 ? message : message.substring(0, 512);
    }

    /** 兼容迁移前 retry_count 为空的历史任务。 */
    private int safeRetryCount(SettlementTaskPO task) {
        return task.getRetryCount() == null ? 0 : task.getRetryCount();
    }
}
