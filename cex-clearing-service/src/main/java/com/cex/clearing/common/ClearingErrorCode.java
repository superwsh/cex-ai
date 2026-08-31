package com.cex.clearing.common;

/** 清算服务业务错误码。 */
public enum ClearingErrorCode {
    INVALID_ACCOUNT_COMMAND(51001, "账户资金指令非法"),
    INSUFFICIENT_AVAILABLE_BALANCE(51002, "可用余额不足"),
    INSUFFICIENT_FROZEN_BALANCE(51003, "冻结余额不足"),
    SETTLEMENT_NOT_FOUND(51004, "结算任务不存在"),
    RECONCILIATION_NOT_FOUND(51005, "对账问题不存在"),
    INVALID_ADMIN_OPERATION(51006, "后台操作不允许");

    private final int code;
    private final String message;

    ClearingErrorCode(int code, String message) {
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
