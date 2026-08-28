package com.cex.order.api.request;

import lombok.Data;

@Data
public class CancelOrderRequest {
    private Long orderId;
    private String clientOrderId;
}
