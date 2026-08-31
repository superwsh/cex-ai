package com.cex.clearing.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.clearing.infrastructure.persistence.entity.SettlementJournalPO;
import org.apache.ibatis.annotations.Mapper;

/** 结算凭证头 Mapper。 */
@Mapper
public interface SettlementJournalMapper extends BaseMapper<SettlementJournalPO> {
}
