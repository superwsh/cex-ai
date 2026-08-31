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
import java.util.Objects;
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

    /**
     * 为新建订单写入提交事件的 Outbox 记录。
     *
     * @param order 已在同一事务中创建的订单
     */
    public void publishOrderCreated(Order order) {
        String eventId = UUID.randomUUID().toString();
        OrderEvent event = OrderEvent.builder()
                .eventId(eventId)
                .orderId(String.valueOf(order.getOrderId()))
                .clientOrderId(order.getClientOrderId())
                .userId(order.getUserId())
                .symbol(order.getSymbol())
                .action(OrderEvent.Action.SUBMIT)
                .side(OrderEvent.OrderSide.valueOf(order.getSide().name()))
                .type(OrderEvent.OrderType.valueOf(order.getType().name()))
                .price(order.getPrice())
                .quantity(order.getQuantity())
                .quoteAmount(order.getQuoteAmount())
                .timeInForce(toKafkaTimeInForce(order))
                .timestamp(System.currentTimeMillis())
                .build();
        insertOutbox(order, EVENT_ORDER_CREATED, event);
    }

    /**
     * 为撤销订单写入取消事件的 Outbox 记录。
     *
     * @param order 已在同一事务中完成撤单状态变更的订单
     */
    public void publishOrderCanceled(Order order) {
        String eventId = UUID.randomUUID().toString();
        OrderEvent event = OrderEvent.builder()
                .eventId(eventId)
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
                .quoteAmount(order.getQuoteAmount())
                .timeInForce(toKafkaTimeInForce(order))
                .timestamp(System.currentTimeMillis())
                .build();
        insertOutbox(order, EVENT_ORDER_CANCELED, event);
    }

    /**
     * 将消息载荷和其相同的幂等标识写入 Outbox，避免重放时产生两个不同事件编号。
     *
     * @param order 事件所属订单
     * @param eventType Outbox 事件类型
     * @param event 将被序列化并投递到 Kafka 的事件
     */
    private void insertOutbox(Order order, String eventType, OrderEvent event) {
        LocalDateTime now = LocalDateTime.now();
        OrderEventOutboxPO outbox = OrderEventOutboxPO.builder()
                .id(snowflakeGenerator.nextId())
                .eventId(Objects.requireNonNull(event.getEventId(), "订单事件编号不能为空"))
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

    /**
     * 将订单域的有效期映射为跨服务消息枚举，并拒绝缺失的核心字段。
     *
     * @param order 需要发布事件的订单
     * @return Kafka 订单事件使用的有效期枚举
     */
    private OrderEvent.TimeInForce toKafkaTimeInForce(Order order) {
        return OrderEvent.TimeInForce.valueOf(
                Objects.requireNonNull(order.getTimeInForce(), "订单有效期不能为空").name());
    }

    /**
     * 将事件序列化为 Outbox 持久化载荷。
     *
     * @param event 待序列化订单事件
     * @return JSON 格式的事件载荷
     */
    private String toJson(OrderEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("事件 JSON 序列化失败", e);
        }
    }
}
