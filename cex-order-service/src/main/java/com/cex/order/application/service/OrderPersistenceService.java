package com.cex.order.application.service;

import com.cex.order.application.command.CreateOrderCommand;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderFactory;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.SymbolConfig;
import lombok.RequiredArgsConstructor;
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
        Order order = orderFactory.createPendingMatchOrder(
                orderId,
                command.getUserId(),
                command.getClientOrderId(),
                command.getSymbol(),
                command.getSide(),
                command.getType(),
                command.getType() == com.cex.order.domain.model.OrderType.MARKET
                        ? null : command.getPrice(),
                command.getType() == com.cex.order.domain.model.OrderType.MARKET
                        && command.getSide() == com.cex.order.domain.model.OrderSide.SELL
                        ? command.getQuantity() : command.getQuantity(),
                command.getQuoteAmount(),
                command.getTimeInForce());
        orderRepository.insert(order);
        eventPublisher.publishOrderCreated(order);
        return CreateOrderResult.of(order);
    }
}
