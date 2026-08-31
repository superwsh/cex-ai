package com.cex.clearing.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.clearing.infrastructure.persistence.entity.AdminOperationAuditPO;
import org.apache.ibatis.annotations.Mapper;

/** 清算后台人工操作审计 Mapper。 */
@Mapper
public interface AdminOperationAuditMapper extends BaseMapper<AdminOperationAuditPO> {
}
