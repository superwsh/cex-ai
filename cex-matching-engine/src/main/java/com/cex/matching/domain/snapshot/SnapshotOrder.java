package com.cex.matching.domain.snapshot;

import com.cex.matching.domain.model.MatchOrder;

import java.math.BigDecimal;
import java.util.Objects;

/** 快照中保存的不可变活动订单。 */
public record SnapshotOrder(long orderId, long userId, MatchOrder.Side side, BigDecimal price,
                            BigDecimal quantity, BigDecimal remainingQuantity, long sequence) {

    /**
     * 校验快照订单的资金精度与身份字段。
     */
    public SnapshotOrder {
        if (orderId <= 0L || userId <= 0L || sequence < 0L || price == null || quantity == null
                || remainingQuantity == null || price.signum() <= 0 || quantity.signum() <= 0
                || remainingQuantity.signum() < 0 || remainingQuantity.compareTo(quantity) > 0) {
            throw new IllegalArgumentException("快照订单字段不合法");
        }
        Objects.requireNonNull(side, "快照订单方向不能为空");
    }
}
