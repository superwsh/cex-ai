package com.cex.common.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

/** 撮合引擎输出的订单状态结果事件。 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderResultEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    private String eventId;
    private String orderId;
    private String symbol;
    private Long sequence;
    private OrderResultType type;
    private BigDecimal filledQuantity;
    private BigDecimal remainingQuantity;
    private Long timestamp;

    public enum OrderResultType { ORDER_REJECTED, ORDER_PARTIALLY_FILLED, ORDER_FILLED, ORDER_CANCELED }
}
