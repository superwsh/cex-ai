package com.cex.matching.application.mapper;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.enums.OrderType;
import com.cex.matching.domain.enums.TimeInForce;
import com.cex.matching.domain.model.MatchOrder;

import java.time.Instant;
import java.util.Objects;

/** 将跨服务订单事件转换为撮合领域订单。 */
public final class OrderEventMapper {

    /**
     * 将提交订单事件转换为可进入撮合器的订单。
     *
     * @param event Kafka 中按交易对顺序接收的提交事件
     * @param sequence 当前交易对的撮合命令序号
     * @return 已完成领域校验的撮合订单
     */
    public MatchOrder toMatchOrder(OrderEvent event, long sequence) {
        validateSubmitEvent(event);
        return MatchOrder.builder()
                .orderId(parseOrderId(event.getOrderId()))
                .userId(Objects.requireNonNull(event.getUserId(), "用户编号不能为空"))
                .symbol(event.getSymbol())
                .side(OrderSide.valueOf(event.getSide().name()))
                .type(OrderType.valueOf(event.getType().name()))
                .price(event.getPrice())
                .quantity(event.getQuantity())
                .quoteAmount(event.getQuoteAmount())
                .timeInForce(TimeInForce.valueOf(event.getTimeInForce().name()))
                .createdAt(Instant.ofEpochMilli(event.getTimestamp()))
                .sequence(sequence)
                .build();
    }

    /**
     * 解析订单编号，防止非法消息进入订单簿。
     *
     * @param orderId 订单服务生成的字符串订单编号
     * @return 大于零的长整型订单编号
     */
    public long parseOrderId(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("订单编号不能为空");
        }
        try {
            long parsedOrderId = Long.parseLong(orderId);
            if (parsedOrderId <= 0) {
                throw new IllegalArgumentException("订单编号必须大于零");
            }
            return parsedOrderId;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("订单编号格式非法: " + orderId, exception);
        }
    }

    /**
     * 校验提交事件包含构造领域订单所需的全部字段。
     *
     * @param event 待校验 Kafka 订单事件
     */
    private void validateSubmitEvent(OrderEvent event) {
        Objects.requireNonNull(event, "订单事件不能为空");
        if (event.getAction() != OrderEvent.Action.SUBMIT) {
            throw new IllegalArgumentException("仅提交事件可以转换为撮合订单");
        }
        if (event.getSymbol() == null || event.getSymbol().isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        Objects.requireNonNull(event.getSide(), "买卖方向不能为空");
        Objects.requireNonNull(event.getType(), "订单类型不能为空");
        Objects.requireNonNull(event.getTimeInForce(), "订单有效期不能为空");
        Objects.requireNonNull(event.getTimestamp(), "事件时间不能为空");
    }
}
