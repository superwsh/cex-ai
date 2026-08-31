package com.cex.clearing.domain.settlement;

/** 结算任务状态。 */
public enum SettlementStatus {
    INIT,
    PROCESSING,
    SUCCESS,
    RETRY,
    FAILED,
    MANUAL_REVIEW
}
