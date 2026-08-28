package com.cex.order.common;

import com.cex.common.core.exception.BizException;
import com.cex.order.common.ErrorCode;

/**
 * 订单状态非法流转
 */
public class OrderStatusInvalidException extends BizException {

    public OrderStatusInvalidException(String message) {
        super(ErrorCode.ORDER_STATUS_INVALID.getCode(), message);
    }
}
