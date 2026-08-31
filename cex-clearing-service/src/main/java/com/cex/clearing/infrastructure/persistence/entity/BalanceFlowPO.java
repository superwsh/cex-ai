package com.cex.clearing.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** 不可变账户资金流水。 */
@Data
@TableName("balance_flow")
public class BalanceFlowPO {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    private String bizType;
    private String bizId;
    private String flowType;
    private Long userId;
    private String asset;
    private BigDecimal availableBefore;
    private BigDecimal availableChange;
    private BigDecimal availableAfter;
    private BigDecimal frozenBefore;
    private BigDecimal frozenChange;
    private BigDecimal frozenAfter;
    private LocalDateTime createdAt;
}
