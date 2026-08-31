package com.cex.clearing.application.service;

import com.cex.clearing.domain.settlement.SettlementException;
import com.cex.common.kafka.event.TradeEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 成交事件结算字段校验测试。 */
class TradeEventValidatorTest {

    private final TradeEventValidator validator = new TradeEventValidator();

    @Test
    void shouldAcceptCompleteTradeEvent() {
        assertThatCode(() -> validator.validate(validEvent())).doesNotThrowAnyException();
    }

    @Test
    void shouldRejectWhenAmountDoesNotMatchPriceAndQuantity() {
        TradeEvent event = validEvent();
        event.setAmount(new BigDecimal("9999"));

        assertThatThrownBy(() -> validator.validate(event))
                .isInstanceOf(SettlementException.class)
                .hasMessageContaining("成交金额与价格乘数量不一致");
    }

    @Test
    void shouldRejectPositiveFeeWithoutFeeAsset() {
        TradeEvent event = validEvent();
        event.setBuyerFee(new BigDecimal("0.001"));
        event.setBuyerFeeAsset(null);

        assertThatThrownBy(() -> validator.validate(event))
                .isInstanceOf(SettlementException.class)
                .hasMessageContaining("买方手续费字段非法");
    }

    private TradeEvent validEvent() {
        return TradeEvent.builder().eventId("event-1").tradeId("T-1").sequence(1L).symbol("BTC_USDT")
                .buyOrderId("B-1").sellOrderId("S-1").buyerUserId(100L).sellerUserId(200L)
                .baseAsset("BTC").quoteAsset("USDT").price(new BigDecimal("100000"))
                .quantity(new BigDecimal("0.1")).amount(new BigDecimal("10000"))
                .buyerFee(BigDecimal.ZERO).buyerFeeAsset("BTC")
                .sellerFee(BigDecimal.ZERO).sellerFeeAsset("USDT").timestamp(System.currentTimeMillis()).build();
    }
}
