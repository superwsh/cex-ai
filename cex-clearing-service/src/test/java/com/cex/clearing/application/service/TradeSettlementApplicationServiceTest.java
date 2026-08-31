package com.cex.clearing.application.service;

import com.cex.clearing.domain.clearing.AccountPosting;
import com.cex.clearing.domain.clearing.ClearingCalculator;
import com.cex.clearing.domain.clearing.ClearingResult;
import com.cex.clearing.domain.clearing.PostingType;
import com.cex.clearing.domain.settlement.SettlementException;
import com.cex.clearing.infrastructure.persistence.entity.AccountBalancePO;
import com.cex.clearing.infrastructure.persistence.entity.BalanceFlowPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementEventOutboxPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementJournalEntryPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementJournalPO;
import com.cex.clearing.infrastructure.persistence.mapper.AccountBalanceMapper;
import com.cex.clearing.infrastructure.persistence.mapper.BalanceFlowMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementEventOutboxMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementJournalEntryMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementJournalMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementTaskMapper;
import com.cex.common.kafka.event.TradeEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Phase 5 结算事务的过账、幂等和失败路径测试。 */
@ExtendWith(MockitoExtension.class)
class TradeSettlementApplicationServiceTest {

    @Mock private SettlementTaskMapper settlementTaskMapper;
    @Mock private AccountBalanceMapper accountBalanceMapper;
    @Mock private BalanceFlowMapper balanceFlowMapper;
    @Mock private SettlementJournalMapper journalMapper;
    @Mock private SettlementJournalEntryMapper journalEntryMapper;
    @Mock private SettlementEventOutboxMapper outboxMapper;
    @Mock private ClearingCalculator clearingCalculator;

    private TradeSettlementApplicationService settlementService;

    @BeforeEach
    void setUp() {
        settlementService = new TradeSettlementApplicationService(settlementTaskMapper, accountBalanceMapper,
                balanceFlowMapper, journalMapper, journalEntryMapper, outboxMapper, clearingCalculator, new ObjectMapper());
    }

    @Test
    void shouldSettleTradeWithBalancesFlowsJournalAndOutboxInOneTransaction() {
        TradeEvent event = tradeEvent();
        when(clearingCalculator.calculate(event)).thenReturn(clearingResult());
        when(settlementTaskMapper.claimForSettlement("T-1")).thenReturn(1);
        when(accountBalanceMapper.selectList(any())).thenReturn(accounts());
        when(accountBalanceMapper.applyChange(anyLong(), anyString(), any(BigDecimal.class), any(BigDecimal.class))).thenReturn(1);
        when(balanceFlowMapper.insert(any(BalanceFlowPO.class))).thenReturn(1);
        when(journalMapper.insert(any(SettlementJournalPO.class))).thenReturn(1);
        when(journalEntryMapper.insert(any(SettlementJournalEntryPO.class))).thenReturn(1);
        when(journalMapper.update(any(), any())).thenReturn(1);
        when(settlementTaskMapper.markSuccess("T-1")).thenReturn(1);
        when(outboxMapper.insert(any(SettlementEventOutboxPO.class))).thenReturn(1);

        settlementService.settle(event);

        verify(accountBalanceMapper, times(5)).applyChange(anyLong(), anyString(), any(BigDecimal.class), any(BigDecimal.class));
        verify(balanceFlowMapper, times(5)).insert(any(BalanceFlowPO.class));
        verify(journalMapper).insert(any(SettlementJournalPO.class));
        verify(journalEntryMapper, times(5)).insert(any(SettlementJournalEntryPO.class));
        verify(journalMapper).update(any(), any());
        verify(settlementTaskMapper).markSuccess("T-1");
        verify(outboxMapper).insert(any(SettlementEventOutboxPO.class));

        ArgumentCaptor<BalanceFlowPO> flowCaptor = ArgumentCaptor.forClass(BalanceFlowPO.class);
        verify(balanceFlowMapper, times(5)).insert(flowCaptor.capture());
        BalanceFlowPO buyerQuote = flowCaptor.getAllValues().get(0);
        assertThat(buyerQuote.getUserId()).isEqualTo(100L);
        assertThat(buyerQuote.getAsset()).isEqualTo("USDT");
        assertThat(buyerQuote.getFrozenBefore()).isEqualByComparingTo("10000");
        assertThat(buyerQuote.getFrozenAfter()).isZero();
        assertThat(flowCaptor.getAllValues()).anySatisfy(flow -> {
            assertThat(flow.getUserId()).isEqualTo(0L);
            assertThat(flow.getAsset()).isEqualTo("BTC");
            assertThat(flow.getAvailableChange()).isEqualByComparingTo("0.001");
        });
    }

    @Test
    void shouldRejectSettlementWhenFrozenBalanceCannotBeDebited() {
        TradeEvent event = tradeEvent();
        when(clearingCalculator.calculate(event)).thenReturn(clearingResult());
        when(settlementTaskMapper.claimForSettlement("T-1")).thenReturn(1);
        when(accountBalanceMapper.selectList(any())).thenReturn(accounts());
        when(journalMapper.insert(any(SettlementJournalPO.class))).thenReturn(1);
        when(accountBalanceMapper.applyChange(anyLong(), anyString(), any(BigDecimal.class), any(BigDecimal.class))).thenReturn(0);

        assertThatThrownBy(() -> settlementService.settle(event))
                .isInstanceOfSatisfying(SettlementException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo("INSUFFICIENT_SETTLEMENT_BALANCE"));

        verify(balanceFlowMapper, never()).insert(any(BalanceFlowPO.class));
        verify(journalEntryMapper, never()).insert(any(SettlementJournalEntryPO.class));
        verify(settlementTaskMapper, never()).markSuccess(anyString());
        verify(outboxMapper, never()).insert(any(SettlementEventOutboxPO.class));
    }

    @Test
    void shouldSkipBalanceChangesWhenTaskWasAlreadyClaimedOrSuccessful() {
        TradeEvent event = tradeEvent();
        when(clearingCalculator.calculate(event)).thenReturn(clearingResult());
        when(settlementTaskMapper.claimForSettlement("T-1")).thenReturn(0);

        settlementService.settle(event);

        verify(accountBalanceMapper, never()).selectList(any());
        verify(balanceFlowMapper, never()).insert(any(BalanceFlowPO.class));
        verify(outboxMapper, never()).insert(any(SettlementEventOutboxPO.class));
    }

    private ClearingResult clearingResult() {
        return new ClearingResult("T-1", List.of(
                new AccountPosting(100L, "USDT", BigDecimal.ZERO, new BigDecimal("-10000"),
                        PostingType.BUYER_QUOTE_FROZEN_DEBIT),
                new AccountPosting(100L, "BTC", new BigDecimal("0.099"), BigDecimal.ZERO,
                        PostingType.BUYER_BASE_AVAILABLE_CREDIT),
                new AccountPosting(200L, "BTC", BigDecimal.ZERO, new BigDecimal("-0.1"),
                        PostingType.SELLER_BASE_FROZEN_DEBIT),
                new AccountPosting(200L, "USDT", new BigDecimal("10000"), BigDecimal.ZERO,
                        PostingType.SELLER_QUOTE_AVAILABLE_CREDIT),
                new AccountPosting(0L, "BTC", new BigDecimal("0.001"), BigDecimal.ZERO,
                        PostingType.PLATFORM_FEE_CREDIT)));
    }

    private List<AccountBalancePO> accounts() {
        return List.of(balance(0L, "BTC", "0", "0"), balance(100L, "USDT", "0", "10000"), balance(100L, "BTC", "0", "0"),
                balance(200L, "BTC", "0", "0.1"), balance(200L, "USDT", "0", "0"));
    }

    private AccountBalancePO balance(Long userId, String asset, String available, String frozen) {
        AccountBalancePO balance = new AccountBalancePO();
        balance.setUserId(userId);
        balance.setAsset(asset);
        balance.setAvailable(new BigDecimal(available));
        balance.setFrozen(new BigDecimal(frozen));
        return balance;
    }

    private TradeEvent tradeEvent() {
        return TradeEvent.builder().eventId("E-1").tradeId("T-1").sequence(1L).symbol("BTC_USDT")
                .buyOrderId("B-1").sellOrderId("S-1").buyerUserId(100L).sellerUserId(200L)
                .baseAsset("BTC").quoteAsset("USDT").price(new BigDecimal("100000"))
                .quantity(new BigDecimal("0.1")).amount(new BigDecimal("10000"))
                .buyerFee(new BigDecimal("0.001")).buyerFeeAsset("BTC")
                .sellerFee(BigDecimal.ZERO).sellerFeeAsset("USDT").timestamp(System.currentTimeMillis()).build();
    }
}
