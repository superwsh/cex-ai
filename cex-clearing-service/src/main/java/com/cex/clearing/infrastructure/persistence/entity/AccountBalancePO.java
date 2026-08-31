package com.cex.clearing.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** 用户资产余额持久化对象。 */
@Data
@TableName("account_balance")
public class AccountBalancePO {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    private Long userId;
    private String asset;
    private BigDecimal available;
    private BigDecimal frozen;
    private Long version;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
