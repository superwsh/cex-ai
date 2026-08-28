package com.cex.order.infrastructure.repository;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.order.infrastructure.id.SnowflakeGenerator;
import com.cex.order.infrastructure.persistence.entity.ProcessedEventPO;
import com.cex.order.infrastructure.persistence.mapper.ProcessedEventMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
@RequiredArgsConstructor
public class ProcessedEventRepository {

    private final ProcessedEventMapper processedEventMapper;
    private final SnowflakeGenerator snowflakeGenerator;

    public boolean exists(String eventId, String consumer) {
        return processedEventMapper.selectCount(new LambdaQueryWrapper<ProcessedEventPO>()
                .eq(ProcessedEventPO::getEventId, eventId)
                .eq(ProcessedEventPO::getConsumer, consumer)) > 0;
    }

    public void save(ProcessedEventPO po) {
        if (po.getId() == null) {
            po.setId(snowflakeGenerator.nextId());
        }
        processedEventMapper.insert(po);
    }

    public ProcessedEventPO build(String eventId, String consumer) {
        return ProcessedEventPO.builder()
                .id(snowflakeGenerator.nextId())
                .eventId(eventId)
                .consumer(consumer)
                .processedAt(LocalDateTime.now())
                .build();
    }
}
