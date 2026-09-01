package com.cex.market.application.service;

import com.cex.common.kafka.event.market.OrderBookDeltaEvent;
import com.cex.market.domain.model.MarketOrderBookSnapshot;

import java.util.List;

/** 从 Kafka WAL 重放订单簿增量的端口。 */
public interface OrderBookEventReplayer {

    /**
     * 从快照后一位重放到指定 Kafka 位点（含）。
     *
     * @param snapshot 作为恢复起点的持久化快照
     * @param targetPartition 需要到达的分区
     * @param targetOffset 需要到达的位点
     * @return 同一交易对、按 Kafka 位点升序排列的增量事件
     */
    List<OrderBookDeltaEvent> replayTo(MarketOrderBookSnapshot snapshot, int targetPartition, long targetOffset);
}
