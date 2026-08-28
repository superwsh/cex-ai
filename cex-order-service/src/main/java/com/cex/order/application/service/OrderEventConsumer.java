package com.cex.order.application.service;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.TradeEvent;
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
 * 幂等:eventId(tradeId) + consumer 唯一约束,重复消息直接忽略
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
        if (event == null || event.getTradeId() == null) {
            return;
        }
        // 幂等:已处理过则忽略(事务内检查,防并发重复)
        if (processedEventRepository.exists(event.getTradeId(), CONSUMER)) {
            log.info("成交事件已处理,忽略: tradeId={}", event.getTradeId());
            return;
        }
        updateOrder(event.getBuyOrderId(), event.getQuantity(), event.getAmount());
        updateOrder(event.getSellOrderId(), event.getQuantity(), event.getAmount());
        // 记录幂等键;id 由 save 内部用雪花 ID 填充,保证单测可直接验证传入的 PO
        processedEventRepository.save(ProcessedEventPO.builder()
                .eventId(event.getTradeId())
                .consumer(CONSUMER)
                .processedAt(LocalDateTime.now())
                .build());
        log.info("成交回报处理完成: tradeId={}, symbol={}, quantity={}, price={}",
                event.getTradeId(), event.getSymbol(), event.getQuantity(), event.getPrice());
    }

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
        } catch (NumberFormatException e) {
            log.warn("订单ID非法: {}", orderIdStr);
        }
    }
}
