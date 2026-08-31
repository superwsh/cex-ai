package com.cex.matching.application.service;

import com.cex.matching.domain.model.OrderBookSnapshot;

import java.util.Optional;

/** 订单簿快照持久化抽象，供恢复编排层读取最近一致状态。 */
public interface MatchingSnapshotRepository {

    /**
     * 原子保存指定交易对的订单簿快照。
     *
     * @param snapshot 已在命令边界创建的订单簿快照
     */
    void save(OrderBookSnapshot snapshot);

    /**
     * 读取指定交易对的最近快照。
     *
     * @param symbol 需要恢复的交易对
     * @return 快照不存在时为空
     */
    Optional<OrderBookSnapshot> load(String symbol);
}
