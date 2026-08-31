package com.cex.clearing.interfaces.dto;

import com.cex.clearing.infrastructure.persistence.entity.SettlementTaskPO;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** 后台结算任务视图。 */
public record SettlementView(String tradeId, String eventId, String symbol, String buyOrderId, String sellOrderId,
                             Long buyerUserId, Long sellerUserId, String baseAsset, String quoteAsset,
                             BigDecimal price, BigDecimal quantity, BigDecimal quoteAmount, String status,
                             Integer retryCount, String errorCode, String errorMessage, Long matchSequence,
                             LocalDateTime createdAt, LocalDateTime settledAt, LocalDateTime updatedAt) {
    /** 从持久化对象生成只读响应。 */
    public static SettlementView from(SettlementTaskPO task) {
        return new SettlementView(task.getTradeId(), task.getEventId(), task.getSymbol(), task.getBuyOrderId(),
                task.getSellOrderId(), task.getBuyerUserId(), task.getSellerUserId(), task.getBaseAsset(),
                task.getQuoteAsset(), task.getPrice(), task.getQuantity(), task.getQuoteAmount(), task.getStatus(),
                task.getRetryCount(), task.getErrorCode(), task.getErrorMessage(), task.getMatchSequence(),
                task.getCreatedAt(), task.getSettledAt(), task.getUpdatedAt());
    }
}
