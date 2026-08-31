package com.cex.clearing.interfaces.dto;

import com.cex.clearing.infrastructure.persistence.entity.BalanceFlowPO;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** 后台不可变资金流水视图。 */
public record BalanceFlowView(Long id, String bizType, String bizId, String flowType, Long userId, String asset,
                              BigDecimal availableBefore, BigDecimal availableChange, BigDecimal availableAfter,
                              BigDecimal frozenBefore, BigDecimal frozenChange, BigDecimal frozenAfter,
                              LocalDateTime createdAt) {
    /** 从持久化对象生成只读响应。 */
    public static BalanceFlowView from(BalanceFlowPO flow) {
        return new BalanceFlowView(flow.getId(), flow.getBizType(), flow.getBizId(), flow.getFlowType(),
                flow.getUserId(), flow.getAsset(), flow.getAvailableBefore(), flow.getAvailableChange(),
                flow.getAvailableAfter(), flow.getFrozenBefore(), flow.getFrozenChange(), flow.getFrozenAfter(),
                flow.getCreatedAt());
    }
}
