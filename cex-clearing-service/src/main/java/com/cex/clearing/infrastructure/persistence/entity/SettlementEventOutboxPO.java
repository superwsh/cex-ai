package com.cex.clearing.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/** 结算成功事件的事务性发件箱记录。 */
@Data
@TableName("settlement_event_outbox")
public class SettlementEventOutboxPO {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    private String eventId;
    private String aggregateId;
    private String topic;
    private String payload;
    private String status;
    private Integer retryCount;
    private LocalDateTime nextRetryTime;
    private String processingToken;
    private String lastError;
    private LocalDateTime createdAt;
    private LocalDateTime publishedAt;
    private LocalDateTime updatedAt;
}
