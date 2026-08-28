package com.cex.order.domain.repository;

import com.cex.order.domain.model.Order;

import java.util.List;

public interface OrderRepository {

    void insert(Order order);

    void update(Order order);

    Order findByOrderId(Long orderId);

    Order findByUserIdAndClientOrderId(Long userId, String clientOrderId);

    /** 当前委托(游标分页):created_at < createdAt OR (created_at = createdAt AND order_id < orderId) */
    List<Order> listOpenOrders(Long userId, String symbol, int limit,
                               java.time.LocalDateTime cursorTime, Long cursorOrderId);

    /** 历史订单(游标分页),status 不限定 */
    List<Order> listHistoryOrders(Long userId, String symbol, int limit,
                                  java.time.LocalDateTime cursorTime, Long cursorOrderId);
}
