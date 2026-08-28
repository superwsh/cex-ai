package com.cex.order.application.service;

import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderResult {

    private Long orderId;
    private OrderStatus status;

    public static CreateOrderResult of(Order order) {
        return CreateOrderResult.builder()
                .orderId(order.getOrderId())
                .status(order.getStatus())
                .build();
    }
}
