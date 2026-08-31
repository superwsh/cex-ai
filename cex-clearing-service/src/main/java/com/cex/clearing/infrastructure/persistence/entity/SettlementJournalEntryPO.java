package com.cex.clearing.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** 复式记账凭证分录，与单条余额过账一一对应。 */
@Data
@TableName("settlement_journal_entry")
public class SettlementJournalEntryPO {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    private String journalId;
    private Long userId;
    private String asset;
    private String accountType;
    private BigDecimal amount;
    private String direction;
    private LocalDateTime createdAt;
}
