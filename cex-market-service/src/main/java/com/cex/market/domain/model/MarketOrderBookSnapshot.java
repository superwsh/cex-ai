package com.cex.market.domain.model;

import java.util.List;

/** 可持久化并用于 Kafka 重放定位的完整订单簿快照。 */
public record MarketOrderBookSnapshot(String symbol, long sequence, List<MarketPriceLevel> bids,
                                      List<MarketPriceLevel> asks, int kafkaPartition, long kafkaOffset,
                                      long createdAt) {

    /**
     * 创建订单簿恢复快照。
     *
     * @param symbol 交易对
     * @param sequence 快照包含的最后盘口序号
     * @param bids 买方全量价格档位
     * @param asks 卖方全量价格档位
     * @param kafkaPartition 对应 Kafka 分区
     * @param kafkaOffset 对应 Kafka 位点
     * @param createdAt 快照创建时间
     */
    public MarketOrderBookSnapshot {
        if (kafkaPartition < 0 || kafkaOffset < 0) {
            throw new IllegalArgumentException("订单簿快照 Kafka 位置信息非法");
        }
        MarketDepthSnapshot depthSnapshot = new MarketDepthSnapshot(symbol, sequence, bids, asks, createdAt);
        bids = depthSnapshot.bids();
        asks = depthSnapshot.asks();
    }
}
