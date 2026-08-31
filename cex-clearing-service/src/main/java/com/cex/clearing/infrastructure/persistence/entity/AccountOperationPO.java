package com.cex.clearing.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/** 账户命令幂等记录。 */
@Data
@TableName("account_operation")
public class AccountOperationPO {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    private String bizType;
    private String bizId;
    private String operationType;
    private Long userId;
    private String asset;
    private LocalDateTime createdAt;
}
