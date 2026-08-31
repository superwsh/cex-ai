package com.cex.matching.application.mapper;

import com.cex.common.kafka.event.TradeEvent;
import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.model.Trade;

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
                .price(trade.getPrice())
                .quantity(trade.getQuantity())
                .amount(trade.getQuoteAmount())
                .timestamp(trade.getTimestamp().toEpochMilli())
                .build();
    }
}
