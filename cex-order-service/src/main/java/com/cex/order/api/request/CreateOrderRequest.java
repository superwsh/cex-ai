package com.cex.order.api.request;

import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class CreateOrderRequest {

    @NotBlank(message = "clientOrderId 不能为空")
    private String clientOrderId;

    @NotBlank(message = "symbol 不能为空")
    private String symbol;

    @NotNull(message = "side 不能为空")
    private OrderSide side;

    @NotNull(message = "type 不能为空")
    private OrderType type;

    @Positive(message = "price 必须大于 0")
    private BigDecimal price;

    @Positive(message = "quantity 必须大于 0")
    private BigDecimal quantity;

    /** 市价买单的冻结金额 */
    @Positive(message = "quoteAmount 必须大于 0")
    private BigDecimal quoteAmount;

    private TimeInForce timeInForce = TimeInForce.GTC;
}
