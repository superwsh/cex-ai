package com.cex.matching.domain.enums;

/** 撮合引擎向下游服务提供的订单和成交事件类型。 */
public enum OrderEventType {
    ORDER_ACCEPTED,
    ORDER_REJECTED,
    ORDER_PARTIALLY_FILLED,
    ORDER_FILLED,
    ORDER_CANCELED,
    TRADE_CREATED
}
