package com.cex.order.domain.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.common.ErrorCode;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * 交易规则校验:精度、最小数量、最小金额
 */
@Component
public class TradingRuleValidator {

    public void validatePrice(BigDecimal price, SymbolConfig config) {
        if (price == null) {
            return; // 市价单无价格
        }
        if (price.scale() > config.getPriceScale()) {
            throw new BizException(ErrorCode.PRICE_SCALE_ERROR.getCode(),
                    ErrorCode.PRICE_SCALE_ERROR.getMessage() + ", 最多 " + config.getPriceScale() + " 位小数");
        }
    }

    public void validateQuantity(BigDecimal quantity, SymbolConfig config) {
        if (quantity == null || quantity.compareTo(BigDecimal.ZERO) <= 0) {
            throw new BizException(ErrorCode.INVALID_PARAM.getCode(), "委托数量必须大于 0");
        }
        if (quantity.scale() > config.getQuantityScale()) {
            throw new BizException(ErrorCode.QUANTITY_SCALE_ERROR.getCode(),
                    ErrorCode.QUANTITY_SCALE_ERROR.getMessage() + ", 最多 " + config.getQuantityScale() + " 位小数");
        }
        if (quantity.compareTo(config.getMinQuantity()) < 0) {
            throw new BizException(ErrorCode.MIN_QUANTITY_NOT_MET.getCode(),
                    ErrorCode.MIN_QUANTITY_NOT_MET.getMessage() + ", 最小 " + config.getMinQuantity());
        }
    }

    /**
     * 最小金额校验:限价买单校验 price*quantity >= minAmount;
     * 市价买单校验 quoteAmount >= minAmount;卖单按数量校验(委托 validateQuantity)
     */
    public void validateMinAmount(OrderSide side, OrderType type,
                                  BigDecimal price, BigDecimal quantity, SymbolConfig config) {
        if (side == OrderSide.SELL) {
            validateQuantity(quantity, config);
            return;
        }
        BigDecimal amount;
        if (type == OrderType.MARKET) {
            if (price == null) {
                throw new BizException(ErrorCode.INVALID_PARAM.getCode(), "市价买单必须传入 quoteAmount");
            }
            amount = price; // 此处 price 参数实际承载 quoteAmount,由调用方传入
        } else {
            amount = price.multiply(quantity);
        }
        if (amount.compareTo(config.getMinAmount()) < 0) {
            throw new BizException(ErrorCode.MIN_AMOUNT_NOT_MET.getCode(),
                    ErrorCode.MIN_AMOUNT_NOT_MET.getMessage() + ", 最小 " + config.getMinAmount());
        }
    }
}
