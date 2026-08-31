package com.cex.matching.domain.model;

import java.util.List;
import java.util.Objects;

/** 指定交易对在单一命令序号点上的订单簿快照。 */
public record OrderBookSnapshot(String symbol, long sequence, long tradeIdHighWaterMark,
                                List<RestingOrderSnapshot> orders,
                                List<ProcessedMatchResultSnapshot> processedResults) {

    /**
     * 兼容未记录成交编号高水位的快照构造方式。
     *
     * @param symbol 快照所属交易对
     * @param sequence 快照最后命令序号
     * @param orders 快照挂单集合
     */
    public OrderBookSnapshot(String symbol, long sequence, List<RestingOrderSnapshot> orders) {
        this(symbol, sequence, 0L, orders, List.of());
    }

    /**
     * 兼容不含命令幂等结果的旧版快照构造方式。
     *
     * @param symbol 快照所属交易对
     * @param sequence 快照最后命令序号
     * @param tradeIdHighWaterMark 成交编号高水位
     * @param orders 快照挂单集合
     */
    public OrderBookSnapshot(String symbol, long sequence, long tradeIdHighWaterMark,
                             List<RestingOrderSnapshot> orders) {
        this(symbol, sequence, tradeIdHighWaterMark, orders, List.of());
    }

    /** 校验快照基础字段并冻结挂单集合。 */
    public OrderBookSnapshot {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("快照交易对不能为空");
        }
        if (sequence < 0) {
            throw new IllegalArgumentException("快照序号不能小于零");
        }
        if (tradeIdHighWaterMark < 0) {
            throw new IllegalArgumentException("成交编号高水位不能小于零");
        }
        orders = List.copyOf(Objects.requireNonNull(orders, "快照订单集合不能为空"));
        for (RestingOrderSnapshot order : orders) {
            if (!symbol.equals(order.symbol())) {
                throw new IllegalArgumentException("快照订单交易对与订单簿不一致");
            }
        }
        processedResults = processedResults == null ? List.of() : List.copyOf(processedResults);
    }
}
