package com.cex.order.application.command;

import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderCommand {

    private Long userId;
    private String clientOrderId;
    private String symbol;
    private OrderSide side;
    private OrderType type;
    /** 限价单价格;市价买单此字段传 quoteAmount */
    private BigDecimal price;
    private BigDecimal quantity;
    /** 市价买单的冻结金额(限价单/市价卖单为 null) */
    private BigDecimal quoteAmount;
    private TimeInForce timeInForce;
}
