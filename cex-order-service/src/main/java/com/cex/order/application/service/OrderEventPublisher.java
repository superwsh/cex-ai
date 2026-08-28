package com.cex.order.application.service;

import com.cex.order.domain.model.Order;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import com.cex.order.infrastructure.repository.OutboxRepository;
import com.cex.order.infrastructure.id.SnowflakeGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.cex.common.kafka.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * 订单领域事件发布:事务内写入 Outbox(本地事务保证订单与事件一致)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventPublisher {

    public static final String EVENT_ORDER_CREATED = "ORDER_CREATED";
    public static final String EVENT_ORDER_CANCELED = "ORDER_CANCELED";

    private final OutboxRepository outboxRepository;
    private final SnowflakeGenerator snowflakeGenerator;
    private final ObjectMapper objectMapper;

    /** 订单创建事件(SUBMIT) */
    public void publishOrderCreated(Order order) {
        OrderEvent event = OrderEvent.builder()
                .orderId(String.valueOf(order.getOrderId()))
                .clientOrderId(order.getClientOrderId())
                .userId(order.getUserId())
                .symbol(order.getSymbol())
                .action(OrderEvent.Action.SUBMIT)
                .side(OrderEvent.OrderSide.valueOf(order.getSide().name()))
                .type(OrderEvent.OrderType.valueOf(order.getType().name()))
                .price(order.getPrice())
                .quantity(order.getQuantity())
                .timestamp(System.currentTimeMillis())
                .build();
        insertOutbox(order, EVENT_ORDER_CREATED, event);
    }

    /** 订单取消事件(CANCEL) */
    public void publishOrderCanceled(Order order) {
        OrderEvent event = OrderEvent.builder()
                .orderId(String.valueOf(order.getOrderId()))
                .clientOrderId(order.getClientOrderId())
                .userId(order.getUserId())
                .symbol(order.getSymbol())
                .action(OrderEvent.Action.CANCEL)
                .side(OrderEvent.OrderSide.valueOf(order.getSide().name()))
                .type(OrderEvent.OrderType.valueOf(order.getType().name()))
                .price(order.getPrice())
                .quantity(order.getQuantity().subtract(
                        order.getFilledQuantity() == null ? java.math.BigDecimal.ZERO : order.getFilledQuantity()))
                .timestamp(System.currentTimeMillis())
                .build();
        insertOutbox(order, EVENT_ORDER_CANCELED, event);
    }

    private void insertOutbox(Order order, String eventType, OrderEvent event) {
        LocalDateTime now = LocalDateTime.now();
        OrderEventOutboxPO outbox = OrderEventOutboxPO.builder()
                .id(snowflakeGenerator.nextId())
                .eventId(UUID.randomUUID().toString())
                .aggregateType("ORDER")
                .aggregateId(String.valueOf(order.getOrderId()))
                .eventType(eventType)
                .payload(toJson(event))
                .status(OrderEventOutboxPO.STATUS_INIT)
                .retryCount(0)
                .nextRetryTime(now)
                .createdAt(now)
                .updatedAt(now)
                .build();
        outboxRepository.insert(outbox);
        log.info("订单事件已写入 Outbox: eventType={}, orderId={}, userId={}, symbol={}",
                eventType, order.getOrderId(), order.getUserId(), order.getSymbol());
    }

    private String toJson(OrderEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("事件 JSON 序列化失败", e);
        }
    }
}
