package com.cex.order.application.service;

import com.cex.common.kafka.event.OrderUnfreezeEvent;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.service.FreezeCalculator;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.id.SnowflakeGenerator;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import com.cex.order.infrastructure.repository.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** 在订单终态的本地事务中写入剩余冻结释放 Outbox。 */
@Component
@RequiredArgsConstructor
public class OrderUnfreezeEventPublisher {

    public static final String EVENT_ORDER_UNFREEZE = "ORDER_UNFREEZE";

    private final OutboxRepository outboxRepository;
    private final SnowflakeGenerator snowflakeGenerator;
    private final ObjectMapper objectMapper;
    private final FreezeCalculator freezeCalculator;

    /** 为终态订单创建唯一的剩余冻结释放事件。 */
    public void publishIfNeeded(Order order, SymbolConfig symbolConfig) {
        BigDecimal amount = freezeCalculator.remainingToUnfreeze(order, symbolConfig);
        if (amount.signum() <= 0) {
            return;
        }
        String eventId = "order-unfreeze-" + order.getOrderId();
        OrderUnfreezeEvent event = OrderUnfreezeEvent.builder().eventId(eventId).orderId(order.getOrderId())
                .userId(order.getUserId()).asset(freezeCalculator.freezeCurrency(order.getSide(), symbolConfig))
                .amount(amount).reason(order.getStatus().name()).timestamp(System.currentTimeMillis()).build();
        LocalDateTime now = LocalDateTime.now();
        outboxRepository.insert(OrderEventOutboxPO.builder().id(snowflakeGenerator.nextId()).eventId(eventId)
                .aggregateType("ORDER").aggregateId(String.valueOf(order.getOrderId()))
                .eventType(EVENT_ORDER_UNFREEZE).payload(toJson(event)).status(OrderEventOutboxPO.STATUS_INIT)
                .retryCount(0).nextRetryTime(now).createdAt(now).updatedAt(now).build());
    }

    /** 序列化解冻事件，失败时回滚调用方订单事务。 */
    private String toJson(OrderUnfreezeEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("订单解冻事件序列化失败", exception);
        }
    }
}
