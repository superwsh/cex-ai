package com.cex.common.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 订单事件：订单服务通过本地消息表模式发布，由撮合引擎消费。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 交易对内严格连续的撮合命令序号；旧版消息可为空。 */
    private Long sequence;

    public enum OrderType {
        LIMIT,   // 限价
        MARKET   // 市价
    }

    public enum OrderSide {
        BUY,
        SELL
    }

    public enum Action {
        SUBMIT,  // 提交
        CANCEL   // 撤销
    }

    public enum TimeInForce {
        GTC,
        IOC,
        FOK
    }

    /** 事件ID（Outbox 生成，用于消费者幂等） */
    private String eventId;

    /** 订单ID（订单服务生成，全局唯一） */
    private String orderId;

    /** 交易对，如 BTC/USDT */
    private String symbol;

    /** 用户ID */
    private Long userId;

    /** 动作：提交 / 撤销 */
    private Action action;

    /** 买卖方向 */
    private OrderSide side;

    /** 订单类型 */
    private OrderType type;

    /** 委托价格（市价单为 null） */
    private BigDecimal price;

    /** 委托数量 */
    private BigDecimal quantity;

    /** 市价买单可使用的计价资产预算 */
    private BigDecimal quoteAmount;

    /** 剩余数量的处置规则 */
    private TimeInForce timeInForce;

    /** 客户端订单号(幂等键,撮合引擎回报时原样带回) */
    private String clientOrderId;

    /** 事件发生时间（毫秒时间戳） */
    private Long timestamp;
}
