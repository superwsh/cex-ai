package com.cex.common.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 撮合成交事件：Matching Engine 发布，Clearing / Market / Notification 消费
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 成交ID（撮合引擎生成，单调递增） */
    private String tradeId;

    /** 交易对，如 BTC/USDT */
    private String symbol;

    /** 买单订单ID */
    private String buyOrderId;

    /** 卖单订单ID */
    private String sellOrderId;

    /** 成交价格 */
    private BigDecimal price;

    /** 成交数量 */
    private BigDecimal quantity;

    /** 成交金额 = price * quantity */
    private BigDecimal amount;

    /** 成交时间（毫秒时间戳） */
    private Long timestamp;
}
