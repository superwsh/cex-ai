package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.enums.OrderType;
import com.cex.matching.domain.enums.TimeInForce;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/** 快照中保存的限价挂单状态，可无损恢复订单簿顺序。 */
public record RestingOrderSnapshot(long orderId, long userId, String symbol, OrderSide side,
                                   BigDecimal price, BigDecimal quantity, BigDecimal remainingQuantity,
                                   TimeInForce timeInForce, Instant createdAt, long sequence) {

    /** 校验快照订单字段，避免错误快照污染恢复后的订单簿。 */
    public RestingOrderSnapshot {
        if (orderId <= 0 || userId <= 0 || sequence < 0) {
            throw new IllegalArgumentException("快照订单标识和序号必须有效");
        }
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("快照交易对不能为空");
        }
        Objects.requireNonNull(side, "快照买卖方向不能为空");
        requirePositive(price, "快照价格");
        requirePositive(quantity, "快照原始数量");
        requirePositive(remainingQuantity, "快照剩余数量");
        if (remainingQuantity.compareTo(quantity) > 0) {
            throw new IllegalArgumentException("快照剩余数量不能超过原始数量");
        }
        Objects.requireNonNull(timeInForce, "快照有效期不能为空");
        Objects.requireNonNull(createdAt, "快照创建时间不能为空");
    }

    /**
     * 从仍在订单簿中的限价订单生成快照对象。
     *
     * @param order 需要持久化的限价挂单
     * @return 可用于恢复的订单快照
     */
    public static RestingOrderSnapshot from(MatchOrder order) {
        Objects.requireNonNull(order, "订单不能为空");
        if (order.getType() != OrderType.LIMIT || !order.hasRemainingToMatch()) {
            throw new IllegalArgumentException("仅可快照仍有剩余数量的限价挂单");
        }
        return new RestingOrderSnapshot(order.getOrderId(), order.getUserId(), order.getSymbol(),
                order.getSide(), order.getPrice(), order.getQuantity(), order.getRemainingQuantity(),
                order.getTimeInForce(), order.getCreatedAt(), order.getSequence());
    }

    /**
     * 将快照订单恢复为领域订单，并重放已成交数量以恢复状态机。
     *
     * @return 状态与剩余数量均已恢复的领域订单
     */
    public MatchOrder toMatchOrder() {
        MatchOrder order = MatchOrder.builder().orderId(orderId).userId(userId).symbol(symbol)
                .side(side).type(OrderType.LIMIT).price(price).quantity(quantity)
                .timeInForce(timeInForce).createdAt(createdAt).sequence(sequence).build();
        BigDecimal filledQuantity = quantity.subtract(remainingQuantity);
        if (filledQuantity.signum() > 0) {
            order.decreaseRemainingQuantity(filledQuantity);
        }
        return order;
    }

    /**
     * 校验金额或数量必须为正数。
     *
     * @param value 待校验数值
     * @param fieldName 字段中文名称
     */
    private static void requirePositive(BigDecimal value, String fieldName) {
        if (value == null || value.signum() <= 0) {
            throw new IllegalArgumentException(fieldName + "必须大于零");
        }
    }
}
