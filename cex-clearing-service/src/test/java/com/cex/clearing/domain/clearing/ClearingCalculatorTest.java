package com.cex.clearing.domain.clearing;

import com.cex.clearing.domain.settlement.SettlementException;
import com.cex.common.kafka.event.TradeEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 现货清算过账及资产守恒测试。 */
class ClearingCalculatorTest {

    private ClearingCalculator calculator;

    @BeforeEach
    void setUp() {
        calculator = new ClearingCalculator(new TradeEventFeeCalculator());
    }

    @Test
    void shouldCalculateBuyerAndSellerPostingsWithoutFees() {
        ClearingResult result = calculator.calculate(trade("0", "0"));

        assertThat(result.postings()).hasSize(4);
        assertPosting(result, PostingType.BUYER_QUOTE_FROZEN_DEBIT, "USDT", "0", "-10000");
        assertPosting(result, PostingType.BUYER_BASE_AVAILABLE_CREDIT, "BTC", "0.1", "0");
        assertPosting(result, PostingType.SELLER_BASE_FROZEN_DEBIT, "BTC", "0", "-0.1");
        assertPosting(result, PostingType.SELLER_QUOTE_AVAILABLE_CREDIT, "USDT", "10000", "0");
    }

    @Test
    void shouldCreditPlatformWhenBuyerFeeUsesBaseAsset() {
        ClearingResult result = calculator.calculate(trade("0.001", "0"));

        assertThat(result.postings()).hasSize(5);
        assertPosting(result, PostingType.BUYER_BASE_AVAILABLE_CREDIT, "BTC", "0.099", "0");
        assertPosting(result, PostingType.PLATFORM_FEE_CREDIT, "BTC", "0.001", "0");
    }

    @Test
    void shouldCreditPlatformWhenSellerFeeUsesQuoteAsset() {
        ClearingResult result = calculator.calculate(trade("0", "10"));

        assertThat(result.postings()).hasSize(5);
        assertPosting(result, PostingType.SELLER_QUOTE_AVAILABLE_CREDIT, "USDT", "9990", "0");
        assertPosting(result, PostingType.PLATFORM_FEE_CREDIT, "USDT", "10", "0");
    }

    @Test
    void shouldRejectFeeThatConsumesEntireReceivedAsset() {
        assertThatThrownBy(() -> calculator.calculate(trade("0.1", "0")))
                .isInstanceOf(SettlementException.class)
                .hasMessageContaining("手续费超过可收取资产");
    }

    @Test
    void shouldRejectUnsupportedBuyerFeeAsset() {
        TradeEvent event = trade("0.001", "0");
        event.setBuyerFeeAsset("USDT");

        assertThatThrownBy(() -> calculator.calculate(event))
                .isInstanceOf(SettlementException.class)
                .hasMessageContaining("买方手续费资产必须为基础资产");
    }

    @Test
    void shouldRejectTradeWhenAmountDoesNotMatchPriceAndQuantity() {
        TradeEvent event = trade("0", "0");
        event.setAmount(new BigDecimal("9999"));

        assertThatThrownBy(() -> calculator.calculate(event))
                .isInstanceOf(SettlementException.class)
                .hasMessageContaining("成交金额与价格、数量不一致");
    }

    private void assertPosting(ClearingResult result, PostingType type, String asset,
                               String availableChange, String frozenChange) {
        AccountPosting posting = result.postings().stream().filter(item -> item.type() == type).findFirst().orElseThrow();
        assertThat(posting.asset()).isEqualTo(asset);
        assertThat(posting.availableChange()).isEqualByComparingTo(availableChange);
        assertThat(posting.frozenChange()).isEqualByComparingTo(frozenChange);
    }

    private TradeEvent trade(String buyerFee, String sellerFee) {
        return TradeEvent.builder().tradeId("T-1").eventId("E-1").sequence(1L).symbol("BTC_USDT")
                .buyOrderId("B-1").sellOrderId("S-1").buyerUserId(100L).sellerUserId(200L)
                .baseAsset("BTC").quoteAsset("USDT").price(new BigDecimal("100000"))
                .quantity(new BigDecimal("0.1")).amount(new BigDecimal("10000"))
                .buyerFee(new BigDecimal(buyerFee)).buyerFeeAsset("BTC")
                .sellerFee(new BigDecimal(sellerFee)).sellerFeeAsset("USDT").timestamp(System.currentTimeMillis()).build();
    }
}
