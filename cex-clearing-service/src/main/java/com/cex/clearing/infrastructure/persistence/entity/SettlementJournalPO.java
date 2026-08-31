package com.cex.clearing.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/** 一笔成交的复式记账凭证头。 */
@Data
@TableName("settlement_journal")
public class SettlementJournalPO {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    private String journalId;
    private String bizType;
    private String bizId;
    private String tradeId;
    private String status;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
