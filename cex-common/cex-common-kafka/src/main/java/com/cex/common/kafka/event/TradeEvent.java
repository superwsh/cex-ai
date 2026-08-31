package com.cex.common.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 撮合成交事件：由撮合引擎发布，供清算、行情和通知服务消费。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 成交ID（撮合引擎生成，单调递增） */
    private String tradeId;

    /** 事件ID（下游消费者幂等键） */
    private String eventId;

    /** 触发成交的撮合命令序号 */
    private Long sequence;

    /** 交易对，如 BTC/USDT */
    private String symbol;

    /** 买单订单ID */
    private String buyOrderId;

    /** 卖单订单ID */
    private String sellOrderId;

    /** 买方用户ID。 */
    private Long buyerUserId;

    /** 卖方用户ID。 */
    private Long sellerUserId;

    /** 基础资产，例如 BTC。 */
    private String baseAsset;

    /** 计价资产，例如 USDT。 */
    private String quoteAsset;

    /** 成交价格 */
    private BigDecimal price;

    /** 成交数量 */
    private BigDecimal quantity;

    /** 成交金额 = price * quantity */
    private BigDecimal amount;

    /** 撮合时已确定的买方手续费；没有手续费时为零。 */
    private BigDecimal buyerFee;

    /** 买方手续费资产。 */
    private String buyerFeeAsset;

    /** 撮合时已确定的卖方手续费；没有手续费时为零。 */
    private BigDecimal sellerFee;

    /** 卖方手续费资产。 */
    private String sellerFeeAsset;

    /** 成交时间（毫秒时间戳） */
    private Long timestamp;
}
