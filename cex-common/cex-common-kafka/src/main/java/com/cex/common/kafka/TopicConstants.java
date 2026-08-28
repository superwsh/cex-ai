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

    /** 资产变更事件，Clearing/Asset Service 对外广播 */
    public static final String TOPIC_ASSET_CHANGE = "cex.asset.change";

    /** 行情 tick（逐笔/深度快照），Market Service 对外广播 */
    public static final String TOPIC_MARKET_TICK = "cex.market.tick";

    /** 用户事件（注册/风控/通知触发） */
    public static final String TOPIC_USER_EVENT = "cex.user.event";
}
