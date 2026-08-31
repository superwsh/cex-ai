package com.cex.clearing.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;
import java.math.BigDecimal;

/** 成交结算任务。 */
@Data
@TableName("settlement_task")
public class SettlementTaskPO {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    private String tradeId;
    private String eventId;
    private String symbol;
    private String buyOrderId;
    private String sellOrderId;
    private Long buyerUserId;
    private Long sellerUserId;
    private String baseAsset;
    private String quoteAsset;
    private BigDecimal price;
    private BigDecimal quantity;
    private BigDecimal quoteAmount;
    private BigDecimal buyerFee;
    private String buyerFeeAsset;
    private BigDecimal sellerFee;
    private String sellerFeeAsset;
    private Long matchSequence;
    private Long tradeTime;
    private String status;
    private Integer retryCount;
    private LocalDateTime nextRetryTime;
    private LocalDateTime processingAt;
    private String errorCode;
    private String errorMessage;
    private LocalDateTime createdAt;
    private LocalDateTime settledAt;
    private LocalDateTime updatedAt;
}
