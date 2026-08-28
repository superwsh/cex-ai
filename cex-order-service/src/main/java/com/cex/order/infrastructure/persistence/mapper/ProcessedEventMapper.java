package com.cex.order.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.order.infrastructure.persistence.entity.ProcessedEventPO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface ProcessedEventMapper extends BaseMapper<ProcessedEventPO> {
}
