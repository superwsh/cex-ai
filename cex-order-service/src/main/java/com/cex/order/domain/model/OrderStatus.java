package com.cex.order.domain.model;

public enum OrderStatus {
    NEW,                // 新建(预留)
    PENDING_MATCH,      // 已提交待撮合
    PARTIALLY_FILLED,   // 部分成交
    FILLED,             // 全部成交
    CANCELED,           // 已取消
    REJECTED;           // 已拒绝

    /** 可取消状态 */
    public boolean canCancel() {
        return this == NEW || this == PENDING_MATCH || this == PARTIALLY_FILLED;
    }

    /** 可成交(回报)状态 */
    public boolean canFill() {
        return this == PENDING_MATCH || this == PARTIALLY_FILLED;
    }

    public boolean isTerminal() {
        return this == FILLED || this == CANCELED || this == REJECTED;
    }
}
