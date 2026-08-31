package com.cex.clearing.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.clearing.infrastructure.persistence.entity.AccountOperationPO;
import org.apache.ibatis.annotations.Mapper;

/** 账户命令幂等记录 Mapper。 */
@Mapper
public interface AccountOperationMapper extends BaseMapper<AccountOperationPO> {
}
