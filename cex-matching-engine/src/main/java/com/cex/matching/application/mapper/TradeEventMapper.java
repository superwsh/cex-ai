package com.cex.matching.application.mapper;

import com.cex.common.kafka.event.TradeEvent;
import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.model.Trade;

import java.math.BigDecimal;

/** 将撮合领域成交记录转换为下游服务使用的公共消息。 */
public final class TradeEventMapper {

    /**
     * 将成交记录映射为包含稳定幂等键的成交事件。
     *
     * @param trade 撮合核心生成的成交记录
     * @return 可供清算等下游服务消费的成交事件
     */
    public TradeEvent toTradeEvent(Trade trade) {
        boolean makerIsBuyer = trade.getMakerSide() == OrderSide.BUY;
        return TradeEvent.builder()
                .eventId("trade-" + trade.getTradeId())
                .tradeId(trade.getTradeId())
                .sequence(trade.getSequence())
                .symbol(trade.getSymbol())
                .buyOrderId(String.valueOf(makerIsBuyer ? trade.getMakerOrderId() : trade.getTakerOrderId()))
                .sellOrderId(String.valueOf(makerIsBuyer ? trade.getTakerOrderId() : trade.getMakerOrderId()))
                .buyerUserId(makerIsBuyer ? trade.getMakerUserId() : trade.getTakerUserId())
                .sellerUserId(makerIsBuyer ? trade.getTakerUserId() : trade.getMakerUserId())
                .baseAsset(trade.getBaseAsset())
                .quoteAsset(trade.getQuoteAsset())
                .price(trade.getPrice())
                .quantity(trade.getQuantity())
                .amount(trade.getQuoteAmount())
                .buyerFee(BigDecimal.ZERO)
                .buyerFeeAsset(trade.getBaseAsset())
                .sellerFee(BigDecimal.ZERO)
                .sellerFeeAsset(trade.getQuoteAsset())
                .timestamp(trade.getTimestamp().toEpochMilli())
                .build();
    }
}
