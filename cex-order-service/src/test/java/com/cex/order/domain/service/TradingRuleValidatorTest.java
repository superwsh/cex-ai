package com.cex.order.domain.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TradingRuleValidatorTest {

    private TradingRuleValidator validator;
    private SymbolConfig config;

    @BeforeEach
    void setUp() {
        validator = new TradingRuleValidator();
        config = SymbolConfig.builder()
                .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
                .priceScale(2).quantityScale(6)
                .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
                .status(SymbolConfig.SymbolStatus.ACTIVE)
                .build();
    }

    @Test
    void validatePrice_scaleExceeded_throws() {
        assertThatThrownBy(() -> validator.validatePrice(new BigDecimal("100000.123"), config))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("价格精度");
    }

    @Test
    void validatePrice_ok() {
        assertThatCode(() -> validator.validatePrice(new BigDecimal("100000.12"), config))
                .doesNotThrowAnyException();
    }

    @Test
    void validateQuantity_scaleExceeded_throws() {
        assertThatThrownBy(() -> validator.validateQuantity(new BigDecimal("0.1234567"), config))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("数量精度");
    }

    @Test
    void validateQuantity_belowMin_throws() {
        assertThatThrownBy(() -> validator.validateQuantity(new BigDecimal("0.00005"), config))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("最小下单数量");
    }

    @Test
    void validateMinAmount_limitBuy_notEnough_throws() {
        assertThatThrownBy(() -> validator.validateMinAmount(
                        OrderSide.BUY, OrderType.LIMIT,
                        new BigDecimal("100"), new BigDecimal("0.01"), config))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("最小下单金额");
    }

    @Test
    void validateMinAmount_limitSell_skipped() {
        // 卖单按数量校验,不校验金额
        assertThatCode(() -> validator.validateMinAmount(
                        OrderSide.SELL, OrderType.LIMIT,
                        new BigDecimal("100"), new BigDecimal("0.01"), config))
                .doesNotThrowAnyException();
    }

    @Test
    void validateMarketBuy_quoteAmountRequired() {
        assertThatThrownBy(() -> validator.validateMinAmount(
                        OrderSide.BUY, OrderType.MARKET,
                        null, BigDecimal.ZERO, config))
                .isInstanceOf(BizException.class);
    }

    @Test
    void validateMarketSell_quantityRequired() {
        assertThatThrownBy(() -> validator.validateMinAmount(
                        OrderSide.SELL, OrderType.MARKET,
                        null, new BigDecimal("0.00005"), config))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("最小下单数量");
    }
}
