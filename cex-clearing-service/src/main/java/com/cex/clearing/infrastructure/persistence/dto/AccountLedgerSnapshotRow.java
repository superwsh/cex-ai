package com.cex.clearing.infrastructure.persistence.dto;

import lombok.Data;

import java.math.BigDecimal;

/** 基于首笔流水快照与累计变动计算的账户账本余额。 */
@Data
public class AccountLedgerSnapshotRow {
    private Long userId;
    private String asset;
    private BigDecimal expectedAvailable;
    private BigDecimal actualAvailable;
    private BigDecimal expectedFrozen;
    private BigDecimal actualFrozen;
}
