package com.cex.order.api.response;

import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderResponse {

    private Long orderId;
    private Long userId;
    private String clientOrderId;
    private String symbol;
    private OrderSide side;
    private OrderType type;
    private BigDecimal price;
    private BigDecimal quantity;
    private BigDecimal quoteAmount;
    private BigDecimal filledQuantity;
    private BigDecimal filledAmount;
    private OrderStatus status;
    private TimeInForce timeInForce;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public static OrderResponse of(Order order) {
        return OrderResponse.builder()
                .orderId(order.getOrderId()).userId(order.getUserId())
                .clientOrderId(order.getClientOrderId()).symbol(order.getSymbol())
                .side(order.getSide()).type(order.getType())
                .price(order.getPrice()).quantity(order.getQuantity())
                .quoteAmount(order.getQuoteAmount())
                .filledQuantity(order.getFilledQuantity()).filledAmount(order.getFilledAmount())
                .status(order.getStatus()).timeInForce(order.getTimeInForce())
                .createdAt(order.getCreatedAt()).updatedAt(order.getUpdatedAt())
                .build();
    }

    /** 分页游标:createdAt_orderId */
    public String toCursor() {
        return createdAt + "_" + orderId;
    }
}
