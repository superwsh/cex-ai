package com.cex.order.application.service;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.TradeEvent;
import com.cex.order.common.OrderStatusInvalidException;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.infrastructure.persistence.entity.ProcessedEventPO;
import com.cex.order.infrastructure.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 成交回报消费者:撮合引擎发布 TradeEvent 到 cex.trade.event,本服务更新订单成交状态
 * 幂等:eventId + consumer 唯一约束,重复消息直接忽略；兼容旧消息时回退使用 tradeId
 * 事务边界:check + 更新 + 记录 processed_event 在同一事务,消费失败回滚无副作用
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    public static final String CONSUMER = "ORDER_STATUS_CONSUMER";

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(topics = TopicConstants.TOPIC_TRADE_EVENT, groupId = "cex-order")
    @Transactional
    public void onTradeEvent(TradeEvent event) {
        String eventId = resolveEventId(event);
        if (eventId == null) {
            return;
        }
        // 幂等:已处理过则忽略(事务内检查,防并发重复)
        if (processedEventRepository.exists(eventId, CONSUMER)) {
            log.info("成交事件已处理,忽略: eventId={}, tradeId={}", eventId, event.getTradeId());
            return;
        }
        updateOrder(event.getBuyOrderId(), event.getQuantity(), event.getAmount());
        updateOrder(event.getSellOrderId(), event.getQuantity(), event.getAmount());
        // 记录幂等键;id 由 save 内部用雪花 ID 填充,保证单测可直接验证传入的 PO
        processedEventRepository.save(ProcessedEventPO.builder()
                .eventId(eventId)
                .consumer(CONSUMER)
                .processedAt(LocalDateTime.now())
                .build());
        log.info("成交回报处理完成: tradeId={}, symbol={}, quantity={}, price={}",
                event.getTradeId(), event.getSymbol(), event.getQuantity(), event.getPrice());
    }

    /**
     * 优先采用生产方提供的事件编号；为平滑处理旧版本消息，缺失时使用成交编号。
     *
     * @param event Kafka 收到的成交事件
     * @return 可用于幂等记录的事件编号；事件无效时返回 null
     */
    private String resolveEventId(TradeEvent event) {
        if (event == null) {
            return null;
        }
        if (event.getEventId() != null && !event.getEventId().isBlank()) {
            return event.getEventId();
        }
        return event.getTradeId();
    }

    /**
     * 根据成交数量和金额推进单个订单的领域状态。
     *
     * @param orderIdStr 消息中的订单编号
     * @param quantity 本次成交数量
     * @param amount 本次成交金额
     */
    private void updateOrder(String orderIdStr, BigDecimal quantity, BigDecimal amount) {
        if (orderIdStr == null) {
            return;
        }
        try {
            Long orderId = Long.valueOf(orderIdStr);
            Order order = orderRepository.findByOrderId(orderId);
            if (order == null) {
                log.warn("成交回报的订单不存在(可能已分库或清理): orderId={}", orderId);
                return;
            }
            order.markPartiallyFilled(quantity, amount); // 状态机:内部判断 PARTIALLY_FILLED/FILLED
            orderRepository.update(order);
        } catch (OrderStatusInvalidException e) {
            // 状态冲突(取消/终态竞态):视为已消费跳过,避免重试风暴,幂等记录照常写入,留日志人工介入
            log.warn("成交回报与订单状态冲突,跳过: orderId={}, reason={}", orderIdStr, e.getMessage());
        } catch (NumberFormatException e) {
            log.warn("订单ID非法: {}", orderIdStr);
        }
    }
}
