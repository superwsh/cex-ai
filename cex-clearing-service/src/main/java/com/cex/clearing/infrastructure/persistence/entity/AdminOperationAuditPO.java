package com.cex.clearing.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/** 清算后台人工操作审计记录。 */
@Data
@TableName("clearing_admin_operation_audit")
public class AdminOperationAuditPO {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    private String operationType;
    private String targetId;
    private String operatorId;
    private String requestId;
    private String reason;
    private String beforeStatus;
    private String afterStatus;
    private LocalDateTime createdAt;
}
