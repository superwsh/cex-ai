package com.cex.clearing.domain.clearing;

import com.cex.common.kafka.event.TradeEvent;

import java.util.Objects;

/** 使用 TradeEvent 中撮合时固化的费用数据。 */
public final class TradeEventFeeCalculator implements FeeCalculator {

    @Override
    public Fee calculateBuyerFee(TradeEvent tradeEvent) {
        Objects.requireNonNull(tradeEvent, "成交事件不能为空");
        return new Fee(tradeEvent.getBuyerFee(), tradeEvent.getBuyerFeeAsset());
    }

    @Override
    public Fee calculateSellerFee(TradeEvent tradeEvent) {
        Objects.requireNonNull(tradeEvent, "成交事件不能为空");
        return new Fee(tradeEvent.getSellerFee(), tradeEvent.getSellerFeeAsset());
    }
}
