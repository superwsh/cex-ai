package com.cex.matching.application.mapper;

import com.cex.common.kafka.event.OrderResultEvent;
import com.cex.matching.domain.enums.OrderEventType;

/** 将撮合领域订单事件转换为公共订单结果事件。 */
public final class OrderResultEventMapper {

    /**
     * 映射可向订单服务发布的终态或成交状态事件。
     *
     * @param event 撮合领域事件
     * @return 不需要对外发布时返回 null
     */
    public OrderResultEvent toOrderResultEvent(com.cex.matching.domain.model.OrderEvent event) {
        OrderResultEvent.OrderResultType type = switch (event.getType()) {
            case ORDER_REJECTED -> OrderResultEvent.OrderResultType.ORDER_REJECTED;
            case ORDER_PARTIALLY_FILLED -> OrderResultEvent.OrderResultType.ORDER_PARTIALLY_FILLED;
            case ORDER_FILLED -> OrderResultEvent.OrderResultType.ORDER_FILLED;
            case ORDER_CANCELED -> OrderResultEvent.OrderResultType.ORDER_CANCELED;
            case ORDER_ACCEPTED, TRADE_CREATED -> null;
        };
        if (type == null) {
            return null;
        }
        return OrderResultEvent.builder().eventId(event.getEventId()).orderId(String.valueOf(event.getOrderId()))
                .symbol(event.getSymbol()).sequence(event.getSequence()).type(type)
                .filledQuantity(event.getFilledQuantity()).remainingQuantity(event.getRemainingQuantity())
                .timestamp(event.getTimestamp().toEpochMilli()).build();
    }
}
