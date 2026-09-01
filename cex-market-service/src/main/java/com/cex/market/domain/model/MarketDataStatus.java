package com.cex.market.domain.model;

/** 行情交易对的本地数据状态。只有 ACTIVE 状态可以对外提供增量盘口。 */
public enum MarketDataStatus {
    INIT,
    RECOVERING,
    ACTIVE,
    INVALID,
    SUSPENDED
}
