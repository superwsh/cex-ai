package com.cex.clearing.interfaces.dto;

import com.cex.clearing.infrastructure.persistence.entity.ReconciliationResultPO;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** 后台对账问题视图。 */
public record ReconciliationView(Long id, String reconciliationType, String bizId, String tradeId, Long userId,
                                 String asset, String checkItem, BigDecimal expectedAmount, BigDecimal actualAmount,
                                 BigDecimal difference, String status, String errorMessage, LocalDateTime createdAt,
                                 LocalDateTime updatedAt, LocalDateTime resolvedAt) {
    /** 从持久化对象生成只读响应。 */
    public static ReconciliationView from(ReconciliationResultPO issue) {
        return new ReconciliationView(issue.getId(), issue.getReconciliationType(), issue.getBizId(),
                issue.getTradeId(), issue.getUserId(), issue.getAsset(), issue.getCheckItem(),
                issue.getExpectedAmount(), issue.getActualAmount(), issue.getDifference(), issue.getStatus(),
                issue.getErrorMessage(), issue.getCreatedAt(), issue.getUpdatedAt(), issue.getResolvedAt());
    }
}
