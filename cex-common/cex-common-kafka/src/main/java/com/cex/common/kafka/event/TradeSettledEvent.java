package com.cex.common.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 清算结算成功事件。
 * 订单服务只能消费此事件推进成交状态，避免在资金尚未落账时提前确认订单成交。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeSettledEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String eventId;
    private String tradeId;
    private String symbol;
    private String buyOrderId;
    private String sellOrderId;
    private BigDecimal price;
    private BigDecimal quantity;
    private BigDecimal amount;
    private BigDecimal buyerFee;
    private String buyerFeeAsset;
    private BigDecimal sellerFee;
    private String sellerFeeAsset;
    private Long settledAt;
}
