package com.cex.order.infrastructure.repository;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.infrastructure.persistence.entity.OrderPO;
import com.cex.order.infrastructure.persistence.mapper.OrderMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class OrderRepositoryImpl implements OrderRepository {

    private static final String OPEN_STATUSES = "'NEW','PENDING_MATCH','PARTIALLY_FILLED'";

    private final OrderMapper orderMapper;

    @Override
    public void insert(Order order) {
        orderMapper.insert(toPO(order));
    }

    @Override
    public void update(Order order) {
        orderMapper.updateById(toPO(order));
    }

    @Override
    public Order findByOrderId(Long orderId) {
        OrderPO po = orderMapper.selectOne(new LambdaQueryWrapper<OrderPO>()
                .eq(OrderPO::getOrderId, orderId));
        return po == null ? null : toDomain(po);
    }

    @Override
    public Order findByUserIdAndClientOrderId(Long userId, String clientOrderId) {
        if (clientOrderId == null) {
            return null;
        }
        OrderPO po = orderMapper.selectOne(new LambdaQueryWrapper<OrderPO>()
                .eq(OrderPO::getUserId, userId)
                .eq(OrderPO::getClientOrderId, clientOrderId));
        return po == null ? null : toDomain(po);
    }

    @Override
    public List<Order> listOpenOrders(Long userId, String symbol, int limit,
                                      LocalDateTime cursorTime, Long cursorOrderId) {
        LambdaQueryWrapper<OrderPO> wrapper = baseCursorWrapper(userId, symbol, cursorTime, cursorOrderId)
                .inSql(OrderPO::getStatus, OPEN_STATUSES);
        return orderMapper.selectList(wrapper.last("LIMIT " + limit)).stream()
                .map(this::toDomain).toList();
    }

    @Override
    public List<Order> listHistoryOrders(Long userId, String symbol, int limit,
                                         LocalDateTime cursorTime, Long cursorOrderId) {
        return orderMapper.selectList(baseCursorWrapper(userId, symbol, cursorTime, cursorOrderId)
                        .last("LIMIT " + limit)).stream()
                .map(this::toDomain).toList();
    }

    private LambdaQueryWrapper<OrderPO> baseCursorWrapper(Long userId, String symbol,
                                                          LocalDateTime cursorTime, Long cursorOrderId) {
        LambdaQueryWrapper<OrderPO> wrapper = new LambdaQueryWrapper<OrderPO>()
                .eq(OrderPO::getUserId, userId);
        if (symbol != null && !symbol.isBlank()) {
            wrapper.eq(OrderPO::getSymbol, symbol);
        }
        if (cursorTime != null && cursorOrderId != null) {
            wrapper.and(w -> w.lt(OrderPO::getCreatedAt, cursorTime)
                    .or(o -> o.eq(OrderPO::getCreatedAt, cursorTime)
                            .lt(OrderPO::getOrderId, cursorOrderId)));
        }
        wrapper.orderByDesc(OrderPO::getCreatedAt).orderByDesc(OrderPO::getOrderId);
        return wrapper;
    }

    private OrderPO toPO(Order order) {
        return OrderPO.builder()
                .id(order.getId()).orderId(order.getOrderId()).userId(order.getUserId())
                .clientOrderId(order.getClientOrderId()).symbol(order.getSymbol())
                .side(order.getSide().name()).type(order.getType().name())
                .price(order.getPrice()).quantity(order.getQuantity())
                .quoteAmount(order.getQuoteAmount())
                .filledQuantity(order.getFilledQuantity()).filledAmount(order.getFilledAmount())
                .status(order.getStatus().name()).timeInForce(order.getTimeInForce() == null ? null : order.getTimeInForce().name())
                .version(order.getVersion()).createdAt(order.getCreatedAt()).updatedAt(order.getUpdatedAt())
                .build();
    }

    private Order toDomain(OrderPO po) {
        return Order.builder()
                .id(po.getId()).orderId(po.getOrderId()).userId(po.getUserId())
                .clientOrderId(po.getClientOrderId()).symbol(po.getSymbol())
                .side(OrderSide.valueOf(po.getSide())).type(OrderType.valueOf(po.getType()))
                .price(po.getPrice()).quantity(po.getQuantity()).quoteAmount(po.getQuoteAmount())
                .filledQuantity(po.getFilledQuantity()).filledAmount(po.getFilledAmount())
                .status(OrderStatus.valueOf(po.getStatus()))
                .timeInForce(po.getTimeInForce() == null ? null : TimeInForce.valueOf(po.getTimeInForce()))
                .version(po.getVersion()).createdAt(po.getCreatedAt()).updatedAt(po.getUpdatedAt())
                .build();
    }
}
