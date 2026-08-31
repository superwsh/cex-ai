package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderSide;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/** 用于在快照中保存成交重投所需字段的不可变记录。 */
public record TradeSnapshot(String tradeId, String symbol, String baseAsset, String quoteAsset,
                            long makerOrderId, long takerOrderId, long makerUserId, long takerUserId,
                            OrderSide makerSide, BigDecimal price, BigDecimal quantity,
                            Instant timestamp, long sequence) {

    /**
     * 校验成交快照字段。
     */
    public TradeSnapshot {
        if (tradeId == null || tradeId.isBlank() || makerOrderId <= 0 || takerOrderId <= 0
                || makerUserId <= 0 || takerUserId <= 0 || sequence < 0) {
            throw new IllegalArgumentException("成交快照标识和序号必须有效");
        }
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("成交快照交易对不能为空");
        }
        if (price == null || price.signum() <= 0 || quantity == null || quantity.signum() <= 0) {
            throw new IllegalArgumentException("成交快照价格和数量必须大于零");
        }
        Objects.requireNonNull(makerSide, "成交快照挂单方向不能为空");
        Objects.requireNonNull(timestamp, "成交快照时间不能为空");
    }

    /**
     * 从领域成交记录创建持久化快照。
     *
     * @param trade 待保存的领域成交记录
     * @return 对应的成交快照
     */
    public static TradeSnapshot from(Trade trade) {
        Objects.requireNonNull(trade, "成交记录不能为空");
        return new TradeSnapshot(trade.getTradeId(), trade.getSymbol(), trade.getBaseAsset(), trade.getQuoteAsset(),
                trade.getMakerOrderId(), trade.getTakerOrderId(), trade.getMakerUserId(), trade.getTakerUserId(),
                trade.getMakerSide(), trade.getPrice(), trade.getQuantity(),
                trade.getTimestamp(), trade.getSequence());
    }

    /**
     * 还原用于重复投递的领域成交记录。
     *
     * @return 等价的领域成交记录
     */
    public Trade toTrade() {
        return Trade.builder().tradeId(tradeId).symbol(symbol).baseAsset(baseAsset).quoteAsset(quoteAsset)
                .makerOrderId(makerOrderId).takerOrderId(takerOrderId).makerUserId(makerUserId).takerUserId(takerUserId)
                .makerSide(makerSide).price(price).quantity(quantity)
                .timestamp(timestamp).sequence(sequence).build();
    }
}
