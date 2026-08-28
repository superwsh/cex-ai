package com.cex.order.common;

import lombok.Getter;

/**
 * 订单系统业务错误码
 */
@Getter
public enum ErrorCode {

    INVALID_PARAM(40001, "参数错误"),
    SYMBOL_NOT_FOUND(40010, "交易对不存在"),
    SYMBOL_PAUSED(40011, "交易对暂停交易"),
    PRICE_SCALE_ERROR(40020, "价格精度错误"),
    QUANTITY_SCALE_ERROR(40021, "数量精度错误"),
    MIN_QUANTITY_NOT_MET(40022, "最小下单数量不满足"),
    MIN_AMOUNT_NOT_MET(40023, "最小下单金额不满足"),
    ORDER_NOT_FOUND(40030, "订单不存在"),
    ORDER_STATUS_INVALID(40031, "订单状态不允许该操作"),
    DUPLICATE_CLIENT_ORDER(40032, "重复的订单请求"),
    FREEZE_FAILED(50010, "资产冻结失败"),
    INSUFFICIENT_BALANCE(50011, "余额不足"),
    ;

    private final int code;
    private final String message;

    ErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
