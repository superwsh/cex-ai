package com.cex.market.application.service;

import com.cex.market.domain.model.DeltaApplyResult;
import com.cex.market.domain.model.MarketOrderBookSnapshot;

/** 一条盘口增量处理后的结果。 */
public record MarketOrderBookProcessingResult(DeltaApplyResult applyResult, boolean recovered,
                                              MarketOrderBookSnapshot snapshot) {

    /**
     * 创建订单簿增量处理结果。
     *
     * @param applyResult 增量处理结果
     * @param recovered 是否经过 Kafka 重放恢复
     * @param snapshot 处理完成后的持久化快照；重复事件时为空
     */
    public MarketOrderBookProcessingResult {
        if (applyResult == null || applyResult == DeltaApplyResult.APPLIED && snapshot == null) {
            throw new IllegalArgumentException("订单簿增量处理结果字段非法");
        }
    }
}
