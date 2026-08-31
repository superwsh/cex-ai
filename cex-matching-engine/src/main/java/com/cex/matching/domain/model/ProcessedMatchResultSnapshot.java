package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderStatus;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

/** 保存已纳入订单簿快照的命令结果，用于 Kafka 重投时避免重复撮合。 */
public record ProcessedMatchResultSnapshot(String eventId, long orderId, OrderStatus finalStatus,
                                           BigDecimal remainingQuantity, BigDecimal remainingQuoteAmount,
                                           List<TradeSnapshot> trades) {

    /**
     * 校验并冻结命令结果快照。
     */
    public ProcessedMatchResultSnapshot {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("幂等事件编号不能为空");
        }
        if (orderId <= 0) {
            throw new IllegalArgumentException("订单编号必须大于零");
        }
        finalStatus = Objects.requireNonNull(finalStatus, "最终订单状态不能为空");
        remainingQuantity = Objects.requireNonNull(remainingQuantity, "剩余数量不能为空");
        trades = List.copyOf(Objects.requireNonNull(trades, "成交快照不能为空"));
    }

    /**
     * 从已处理的撮合结果创建幂等快照。
     *
     * @param eventId Kafka 订单事件编号
     * @param result 已完成的撮合结果
     * @return 可持久化的结果快照
     */
    public static ProcessedMatchResultSnapshot from(String eventId, MatchResult result) {
        Objects.requireNonNull(result, "撮合结果不能为空");
        return new ProcessedMatchResultSnapshot(eventId, result.getOrderId(), result.getFinalStatus(),
                result.getRemainingQuantity(), result.getRemainingQuoteAmount(),
                result.getTrades().stream().map(TradeSnapshot::from).toList());
    }

    /**
     * 还原重投时需要返回的撮合结果。
     * 订单状态事件不需要再次下发，因此恢复结果不重复包含领域事件。
     *
     * @return 可用于成交事件重投的撮合结果
     */
    public MatchResult toMatchResult() {
        return new MatchResult(orderId, finalStatus, remainingQuantity, remainingQuoteAmount,
                trades.stream().map(TradeSnapshot::toTrade).toList(), List.of());
    }
}
