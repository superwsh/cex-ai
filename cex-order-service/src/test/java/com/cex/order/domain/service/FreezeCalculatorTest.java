package com.cex.order.domain.service;

import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class FreezeCalculatorTest {

    private FreezeCalculator calculator;
    private SymbolConfig config;

    @BeforeEach
    void setUp() {
        calculator = new FreezeCalculator();
        config = SymbolConfig.builder()
                .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
                .priceScale(2).quantityScale(6)
                .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
                .status(SymbolConfig.SymbolStatus.ACTIVE)
                .build();
    }

    @Test
    void limitBuy_freezePriceTimesQuantity() {
        BigDecimal amount = calculator.calculate(
                OrderSide.BUY, OrderType.LIMIT,
                new BigDecimal("100000"), new BigDecimal("0.1"), null);
        assertThat(amount).isEqualByComparingTo("10000");
    }

    @Test
    void limitSell_freezeQuantity() {
        BigDecimal amount = calculator.calculate(
                OrderSide.SELL, OrderType.LIMIT,
                new BigDecimal("100000"), new BigDecimal("0.1"), null);
        assertThat(amount).isEqualByComparingTo("0.1");
    }

    @Test
    void marketBuy_freezeQuoteAmount() {
        BigDecimal amount = calculator.calculate(
                OrderSide.BUY, OrderType.MARKET,
                null, BigDecimal.ZERO, new BigDecimal("5000"));
        assertThat(amount).isEqualByComparingTo("5000");
    }

    @Test
    void marketSell_freezeQuantity() {
        BigDecimal amount = calculator.calculate(
                OrderSide.SELL, OrderType.MARKET,
                null, new BigDecimal("0.5"), null);
        assertThat(amount).isEqualByComparingTo("0.5");
    }

    @Test
    void freezeCurrency_buyIsQuote_sellIsBase() {
        assertThat(calculator.freezeCurrency(OrderSide.BUY, config)).isEqualTo("USDT");
        assertThat(calculator.freezeCurrency(OrderSide.SELL, config)).isEqualTo("BTC");
    }

    @Test
    void remainingToUnfreeze_buy_unfilledPart() {
        Order order = Order.builder()
                .orderId(1L).userId(100L).symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .filledQuantity(new BigDecimal("0.04")).filledAmount(new BigDecimal("4000"))
                .status(OrderStatus.CANCELED)
                .build();
        BigDecimal remaining = calculator.remainingToUnfreeze(order, config);
        assertThat(remaining).isEqualByComparingTo("6000");
    }

    @Test
    void remainingToUnfreeze_sell_unfilledPart() {
        Order order = Order.builder()
                .orderId(1L).userId(100L).symbol("BTC_USDT")
                .side(OrderSide.SELL).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .filledQuantity(new BigDecimal("0.04")).filledAmount(new BigDecimal("4000"))
                .status(OrderStatus.CANCELED)
                .build();
        BigDecimal remaining = calculator.remainingToUnfreeze(order, config);
        assertThat(remaining).isEqualByComparingTo("0.06");
    }
}
