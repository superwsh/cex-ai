package com.cex.matching.domain.enums;

/** 撮合订单的状态机。 */
public enum OrderStatus {
    OPEN,
    PARTIALLY_FILLED,
    FILLED,
    CANCELED,
    REJECTED;

    /** 判断当前状态是否允许继续成交。 */
    public boolean canFill() {
        return this == OPEN || this == PARTIALLY_FILLED;
    }

    /** 判断当前状态是否允许撤销。 */
    public boolean canCancel() {
        return this == OPEN || this == PARTIALLY_FILLED;
    }

    /** 判断当前状态是否为终态。 */
    public boolean isTerminal() {
        return this == FILLED || this == CANCELED || this == REJECTED;
    }
}
