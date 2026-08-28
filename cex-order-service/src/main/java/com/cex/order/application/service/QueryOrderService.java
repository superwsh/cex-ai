package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.api.response.OrderResponse;
import com.cex.order.api.response.PageResult;
import com.cex.order.common.ErrorCode;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 订单查询服务:游标分页(created_at + order_id),不用 offset 深翻页
 */
@Service
@RequiredArgsConstructor
public class QueryOrderService {

    private final OrderRepository orderRepository;

    public OrderResponse getOrder(Long userId, Long orderId) {
        Order order = orderRepository.findByOrderId(orderId);
        if (order == null || !userId.equals(order.getUserId())) {
            throw new BizException(ErrorCode.ORDER_NOT_FOUND.getCode(),
                    ErrorCode.ORDER_NOT_FOUND.getMessage() + ": " + orderId);
        }
        return OrderResponse.of(order);
    }

    public PageResult<OrderResponse> listOpenOrders(Long userId, String symbol,
                                                    String cursor, Integer limit) {
        return page(orderRepository.listOpenOrders(userId, symbol,
                limitOrDefault(limit), cursorTime(cursor), cursorOrderId(cursor)), limitOrDefault(limit));
    }

    public PageResult<OrderResponse> listHistoryOrders(Long userId, String symbol,
                                                       String cursor, Integer limit) {
        return page(orderRepository.listHistoryOrders(userId, symbol,
                limitOrDefault(limit), cursorTime(cursor), cursorOrderId(cursor)), limitOrDefault(limit));
    }

    private PageResult<OrderResponse> page(List<Order> orders, int limit) {
        List<OrderResponse> items = orders.stream().map(OrderResponse::of).toList();
        String nextCursor = items.size() >= limit ? items.get(items.size() - 1).toCursor() : null;
        return PageResult.<OrderResponse>builder().items(items).nextCursor(nextCursor).build();
    }

    private int limitOrDefault(Integer limit) {
        return limit == null || limit <= 0 || limit > 100 ? 20 : limit;
    }

    private LocalDateTime cursorTime(String cursor) {
        return cursor == null || !cursor.contains("_") ? null : LocalDateTime.parse(cursor.split("_")[0]);
    }

    private Long cursorOrderId(String cursor) {
        return cursor == null || !cursor.contains("_") ? null : Long.valueOf(cursor.split("_")[1]);
    }
}
