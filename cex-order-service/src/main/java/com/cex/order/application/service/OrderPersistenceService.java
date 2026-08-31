package com.cex.order.application.service;

import com.cex.order.application.command.CreateOrderCommand;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderFactory;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.SymbolConfig;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 订单落库服务:INSERT orders + INSERT order_event_outbox 在同一本地事务
 * 注意:Kafka 发送不允许出现在本事务内,由 Outbox Relay 负责
 */
@Service
@RequiredArgsConstructor
public class OrderPersistenceService {

    private final OrderRepository orderRepository;
    private final OrderFactory orderFactory;
    private final OrderEventPublisher eventPublisher;

    @Transactional
    public CreateOrderResult createOrderInTx(CreateOrderCommand command, Long orderId, SymbolConfig config) {
        OrderType type = command.getType();
        OrderSide side = command.getSide();
        Order order = orderFactory.createPendingMatchOrder(
                orderId,
                command.getUserId(),
                command.getClientOrderId(),
                command.getSymbol(),
                command.getSide(),
                command.getType(),
                type == OrderType.MARKET ? null : command.getPrice(),
                // 市价买单 quantity 恒为 ZERO(冻结金额语义在 quoteAmount),避免 null 触发 NOT NULL 约束;
                // 市价卖单仍以 quantity 为准
                type == OrderType.MARKET && side == OrderSide.BUY
                        ? BigDecimal.ZERO : command.getQuantity(),
                command.getQuoteAmount(),
                command.getTimeInForce());
        orderRepository.insert(order);
        eventPublisher.publishOrderCreated(order, config);
        return CreateOrderResult.of(order);
    }

    /**
     * 取消落库:状态置 CANCELED + 写取消事件,同一事务
     */
    @Transactional
    public void cancelInTx(Order order, SymbolConfig config) {
        order.setUpdatedAt(java.time.LocalDateTime.now());
        orderRepository.update(order);
        eventPublisher.publishOrderCanceled(order, config);
    }
}
