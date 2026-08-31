package com.cex.clearing.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.clearing.infrastructure.persistence.entity.BalanceFlowPO;
import org.apache.ibatis.annotations.Mapper;

/** 余额流水 Mapper。 */
@Mapper
public interface BalanceFlowMapper extends BaseMapper<BalanceFlowPO> {
}
