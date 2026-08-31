package com.cex.clearing.domain.clearing;

import com.cex.common.kafka.event.TradeEvent;

/** 从固定成交事实获取手续费；不得根据用户当前状态重算历史费用。 */
public interface FeeCalculator {

    /** 计算买方已固定手续费。 */
    Fee calculateBuyerFee(TradeEvent tradeEvent);

    /** 计算卖方已固定手续费。 */
    Fee calculateSellerFee(TradeEvent tradeEvent);
}
