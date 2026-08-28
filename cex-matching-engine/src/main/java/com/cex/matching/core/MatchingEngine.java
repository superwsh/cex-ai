package com.cex.matching.core;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.common.kafka.event.TradeEvent;

import java.util.function.Consumer;

/**
 * 撮合引擎核心接口
 *
 * 设计约束（对应架构设计）：
 * - Symbol Partition：每个交易对（symbol）一个独立引擎实例/线程，不同交易对互不阻塞
 * - Single Thread：同一交易对内串行处理，消除锁竞争，最大化吞吐
 * - In-Memory Book：订单簿常驻内存（价格优先 + 时间优先，红黑树/跳表实现）
 * - 输入：OrderEvent（Kafka，按 symbol 分区保证顺序）
 * - 输出：TradeEvent（Kafka）由回调发布
 * - 容错：内存状态可通过 Kafka 回放重建；与订单服务定期对账
 */
public interface MatchingEngine {

    /**
     * 撮合一笔订单事件
     *
     * @param event   订单事件（SUBMIT / CANCEL）
     * @param onTrade 成交回调：调用方负责将 TradeEvent 发布到 Kafka
     */
    void match(OrderEvent event, Consumer<TradeEvent> onTrade);
}
