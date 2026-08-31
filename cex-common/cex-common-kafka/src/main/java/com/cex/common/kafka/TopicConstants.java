package com.cex.common.kafka;

/**
 * Kafka Topic 常量定义（事件驱动架构的契约）
 */
public final class TopicConstants {

    private TopicConstants() {
    }

    /** 订单事件（创建/撤销/成交回报），Order Service -> Matching Engine */
    public static final String TOPIC_ORDER_EVENT = "cex.order.event";

    /** 撮合成交事件，Matching Engine -> Clearing / Market / Notification */
    public static final String TOPIC_TRADE_EVENT = "cex.trade.event";

    /** 成交事件重试主题。 */
    public static final String TOPIC_TRADE_EVENT_RETRY = "cex.trade.event.retry";

    /** 无法自动恢复的成交事件死信主题。 */
    public static final String TOPIC_TRADE_EVENT_DLQ = "cex.trade.event.dlq";

    /** 清算结算成功事件，Clearing Service -> Order / Market / Notification。 */
    public static final String TOPIC_TRADE_SETTLED_EVENT = "cex.trade.settled";

    /** 撮合订单状态结果，Matching Engine -> Order Service。 */
    public static final String TOPIC_ORDER_RESULT_EVENT = "cex.matching.order-result";

    /** 订单终态后的剩余冻结释放指令，Order Service -> Clearing Service。 */
    public static final String TOPIC_ORDER_UNFREEZE_EVENT = "cex.order.unfreeze";

    /** 资产变更事件，Clearing/Asset Service 对外广播 */
    public static final String TOPIC_ASSET_CHANGE = "cex.asset.change";

    /** 行情 tick（逐笔/深度快照），Market Service 对外广播 */
    public static final String TOPIC_MARKET_TICK = "cex.market.tick";

    /** 用户事件（注册/风控/通知触发） */
    public static final String TOPIC_USER_EVENT = "cex.user.event";
}
