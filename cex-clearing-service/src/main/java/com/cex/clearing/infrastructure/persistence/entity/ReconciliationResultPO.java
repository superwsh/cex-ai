package com.cex.clearing.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** 对账发现的问题记录；只记录差异，不直接变更资金。 */
@Data
@TableName("reconciliation_result")
public class ReconciliationResultPO {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    private String reconciliationType;
    private String bizId;
    private String tradeId;
    private Long userId;
    private String asset;
    private String checkItem;
    private BigDecimal expectedAmount;
    private BigDecimal actualAmount;
    private BigDecimal difference;
    private String status;
    private String errorMessage;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private LocalDateTime resolvedAt;
}
