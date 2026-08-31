package com.cex.clearing.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.clearing.infrastructure.persistence.entity.SettlementJournalEntryPO;
import org.apache.ibatis.annotations.Mapper;

/** 结算凭证分录 Mapper。 */
@Mapper
public interface SettlementJournalEntryMapper extends BaseMapper<SettlementJournalEntryPO> {
}
