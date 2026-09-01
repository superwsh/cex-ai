package com.cex.market.domain.kline;

import java.util.List;

/** 一笔成交应用至全部 KLine 周期后的结果。 */
public record KLineAggregationResult(List<KLine> currentKlines, List<KLine> closedKlines) {

    /**
     * 创建不可修改的 KLine 聚合结果。
     *
     * @param currentKlines 当前未收线 KLine
     * @param closedKlines 本次已收线 KLine
     */
    public KLineAggregationResult {
        if (currentKlines == null || closedKlines == null) {
            throw new IllegalArgumentException("KLine 聚合结果不能为空");
        }
        currentKlines = List.copyOf(currentKlines);
        closedKlines = List.copyOf(closedKlines);
    }
}
