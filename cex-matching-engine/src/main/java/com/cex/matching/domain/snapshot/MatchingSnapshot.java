package com.cex.matching.domain.snapshot;

import com.cex.matching.domain.model.MatchOrder;

import java.util.List;

/** 与单个交易对最后处理序列严格对应的订单簿快照。 */
public record MatchingSnapshot(String symbol, long lastSequence, long snapshotTimestamp,
                               List<SnapshotOrder> buyOrders, List<SnapshotOrder> sellOrders) {

    /**
     * 校验并防御性复制快照内容。
     */
    public MatchingSnapshot {
        if (symbol == null || symbol.isBlank() || lastSequence <= 0L || snapshotTimestamp < 0L) {
            throw new IllegalArgumentException("撮合快照字段不合法");
        }
        buyOrders = List.copyOf(buyOrders);
        sellOrders = List.copyOf(sellOrders);
        validateSide(buyOrders, MatchOrder.Side.BUY);
        validateSide(sellOrders, MatchOrder.Side.SELL);
    }

    /**
     * 校验订单列表中的方向与所属盘口一致。
     *
     * @param orders 待校验订单列表
     * @param expectedSide 盘口应包含的订单方向
     */
    private static void validateSide(List<SnapshotOrder> orders, MatchOrder.Side expectedSide) {
        if (orders.stream().anyMatch(order -> order.side() != expectedSide)) {
            throw new IllegalArgumentException("快照订单方向与盘口不一致");
        }
    }
}
