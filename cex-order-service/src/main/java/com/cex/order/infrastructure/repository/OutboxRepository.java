package com.cex.order.infrastructure.repository;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import com.cex.order.infrastructure.persistence.mapper.OrderEventOutboxMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class OutboxRepository {

    private final OrderEventOutboxMapper outboxMapper;

    public void insert(OrderEventOutboxPO outbox) {
        outboxMapper.insert(outbox);
    }

    public void update(OrderEventOutboxPO outbox) {
        outboxMapper.updateById(outbox);
    }

    /** 扫描待发送记录:INIT/SENDING 且到重试时间 且未超上限 */
    public List<OrderEventOutboxPO> findPending(int limit, int maxRetry) {
        return outboxMapper.selectList(new LambdaQueryWrapper<OrderEventOutboxPO>()
                .in(OrderEventOutboxPO::getStatus,
                        OrderEventOutboxPO.STATUS_INIT, OrderEventOutboxPO.STATUS_SENDING)
                .le(OrderEventOutboxPO::getNextRetryTime, LocalDateTime.now())
                .lt(OrderEventOutboxPO::getRetryCount, maxRetry)
                .orderByAsc(OrderEventOutboxPO::getId)
                .last("LIMIT " + limit));
    }
}
