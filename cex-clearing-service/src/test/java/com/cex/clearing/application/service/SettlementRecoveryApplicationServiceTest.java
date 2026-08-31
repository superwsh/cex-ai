package com.cex.clearing.application.service;

import com.cex.clearing.domain.settlement.SettlementException;
import com.cex.clearing.infrastructure.kafka.TradeEventRetryPublisher;
import com.cex.clearing.infrastructure.persistence.entity.SettlementTaskPO;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementTaskMapper;
import com.cex.clearing.application.observability.ClearingAlertService;
import com.cex.clearing.application.observability.ClearingMetrics;
import com.cex.common.kafka.event.TradeEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Phase 8 超时恢复、持久化重试和死信协同测试。 */
@ExtendWith(MockitoExtension.class)
class SettlementRecoveryApplicationServiceTest {

    @Mock private SettlementTaskMapper settlementTaskMapper;
    @Mock private SettlementTaskApplicationService settlementTaskApplicationService;
    @Mock private TradeSettlementApplicationService tradeSettlementApplicationService;
    @Mock private TradeEventValidator tradeEventValidator;
    @Mock private TradeEventRetryPublisher retryPublisher;
    @Mock private ClearingMetrics clearingMetrics;
    @Mock private ClearingAlertService clearingAlertService;

    private SettlementRecoveryApplicationService recoveryApplicationService;

    @BeforeEach
    void setUp() {
        recoveryApplicationService = new SettlementRecoveryApplicationService(settlementTaskMapper,
                settlementTaskApplicationService, tradeSettlementApplicationService, tradeEventValidator, retryPublisher,
                clearingMetrics, clearingAlertService);
        when(settlementTaskMapper.recoverExpiredProcessing(any(), anyInt(), any())).thenReturn(0);
        when(settlementTaskMapper.recoverExpiredInit(any(), any())).thenReturn(0);
    }

    /** 到期 RETRY 必须基于结算任务快照重建成交事件并重新进入结算事务。 */
    @Test
    void shouldRetryDueTaskFromPersistedTradeSnapshot() {
        SettlementTaskPO task = retryTask();
        when(settlementTaskMapper.selectDueRetryTasks(any(), eq(100))).thenReturn(List.of(task));

        recoveryApplicationService.recoverAndRetry();

        ArgumentCaptor<TradeEvent> eventCaptor = ArgumentCaptor.forClass(TradeEvent.class);
        verify(tradeSettlementApplicationService).settle(eventCaptor.capture());
        assertThat(eventCaptor.getValue().getTradeId()).isEqualTo("T-1");
        assertThat(eventCaptor.getValue().getTimestamp()).isEqualTo(1_700_000_000_000L);
        verify(retryPublisher, never()).publishDlq(any(), anyString());
    }

    /** 恢复重试耗尽必须同时保留 MANUAL_REVIEW 状态并投递 DLQ。 */
    @Test
    void shouldPublishDlqWhenRecoveredRetryIsExhausted() {
        SettlementTaskPO task = retryTask();
        when(settlementTaskMapper.selectDueRetryTasks(any(), eq(100))).thenReturn(List.of(task));
        doThrow(new SettlementException("DATABASE_TIMEOUT", "数据库超时", true))
                .when(tradeSettlementApplicationService).settle(any(TradeEvent.class));
        when(settlementTaskApplicationService.scheduleRetry(eq("T-1"), eq("DATABASE_TIMEOUT"), eq("数据库超时")))
                .thenReturn(new SettlementTaskApplicationService.RetryScheduleResult(true, 5, true));

        recoveryApplicationService.recoverAndRetry();

        verify(retryPublisher).publishDlq(any(TradeEvent.class), eq("DATABASE_TIMEOUT"));
    }

    /** 快照校验失败是永久问题，必须转人工复核和 DLQ，禁止重复执行资金结算。 */
    @Test
    void shouldMarkManualReviewWhenPersistedSnapshotIsInvalid() {
        SettlementTaskPO task = retryTask();
        when(settlementTaskMapper.selectDueRetryTasks(any(), eq(100))).thenReturn(List.of(task));
        doThrow(new SettlementException("INVALID_TRADE", "成交字段非法", false))
                .when(tradeEventValidator).validate(any(TradeEvent.class));

        recoveryApplicationService.recoverAndRetry();

        verify(settlementTaskApplicationService).markManualReview("T-1", "INVALID_TRADE", "成交字段非法");
        verify(retryPublisher).publishDlq(any(TradeEvent.class), eq("INVALID_TRADE"));
        verify(tradeSettlementApplicationService, never()).settle(any());
    }

    /** 构造可由持久化快照恢复的任务。 */
    private SettlementTaskPO retryTask() {
        SettlementTaskPO task = new SettlementTaskPO();
        task.setTradeId("T-1");
        task.setEventId("E-1");
        task.setMatchSequence(1L);
        task.setTradeTime(1_700_000_000_000L);
        task.setSymbol("BTC-USDT");
        task.setBuyOrderId("B-1");
        task.setSellOrderId("S-1");
        task.setBuyerUserId(100L);
        task.setSellerUserId(200L);
        task.setBaseAsset("BTC");
        task.setQuoteAsset("USDT");
        task.setPrice(new BigDecimal("100000"));
        task.setQuantity(new BigDecimal("0.1"));
        task.setQuoteAmount(new BigDecimal("10000"));
        task.setBuyerFee(BigDecimal.ZERO);
        task.setBuyerFeeAsset("BTC");
        task.setSellerFee(BigDecimal.ZERO);
        task.setSellerFeeAsset("USDT");
        task.setRetryCount(1);
        task.setCreatedAt(LocalDateTime.now());
        return task;
    }
}
