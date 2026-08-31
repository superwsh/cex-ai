package com.cex.order.domain.service;

import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * 冻结金额计算
 * 买单:冻结计价币(USDT);卖单:冻结基础币(BTC)
 */
@Component
public class FreezeCalculator {

    /**
     * 下单冻结金额
     *
     * @param quoteAmount 市价买单的冻结金额
     */
    public BigDecimal calculate(OrderSide side, OrderType type,
                                BigDecimal price, BigDecimal quantity, BigDecimal quoteAmount) {
        if (type == OrderType.MARKET && side == OrderSide.BUY) {
            return quoteAmount;
        }
        if (side == OrderSide.SELL) {
            return quantity;
        }
        return price.multiply(quantity);
    }

    public String freezeCurrency(OrderSide side, SymbolConfig config) {
        return side == OrderSide.BUY ? config.getQuoteCurrency() : config.getBaseCurrency();
    }

    /**
     * 取消订单时剩余解冻金额:未成交部分的冻结金额
     */
    public BigDecimal remainingToUnfreeze(Order order, SymbolConfig config) {
        BigDecimal initialFrozen = calculate(order.getSide(), order.getType(), order.getPrice(),
                order.getQuantity(), order.getQuoteAmount());
        BigDecimal consumed = order.getSide() == OrderSide.BUY
                ? safe(order.getFilledAmount()) : safe(order.getFilledQuantity());
        BigDecimal remaining = initialFrozen.subtract(consumed);
        return remaining.signum() < 0 ? BigDecimal.ZERO : remaining;
    }

    private BigDecimal safe(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }
}
