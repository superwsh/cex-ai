package com.cex.market.application.service;

import com.cex.market.domain.model.MarketOrderBookSnapshot;

import java.util.List;

/** 订单簿恢复快照的持久化端口。 */
public interface MarketOrderBookSnapshotRepository {

    /**
     * 幂等保存一个交易对的最新订单簿快照。
     *
     * @param snapshot 已应用增量后的完整快照
     */
    void save(MarketOrderBookSnapshot snapshot);

    /**
     * 查询指定交易对的最近快照。
     *
     * @param symbol 交易对
     * @return 快照；不存在时为空
     */
    MarketOrderBookSnapshot findBySymbol(String symbol);

    /**
     * 查询全部交易对的最近快照，用于启动恢复。
     *
     * @return 快照列表
     */
    List<MarketOrderBookSnapshot> findAll();
}
