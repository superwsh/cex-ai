package com.cex.clearing.interfaces.consumer;

import com.cex.clearing.application.service.SettlementTaskApplicationService;
import com.cex.clearing.application.service.TradeSettlementApplicationService;
import com.cex.clearing.domain.settlement.SettlementException;
import com.cex.clearing.infrastructure.kafka.TradeEventRetryPublisher;
import com.cex.clearing.application.observability.ClearingAlertService;
import com.cex.clearing.application.observability.ClearingMetrics;
import com.cex.common.kafka.event.TradeEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;

/** Phase 3 成交消费者的重试、死信和幂等建档测试。 */
@ExtendWith(MockitoExtension.class)
class TradeExecutedConsumerTest {

    @Mock private SettlementTaskApplicationService settlementTaskApplicationService;
    @Mock private TradeSettlementApplicationService tradeSettlementApplicationService;
    @Mock private TradeEventRetryPublisher retryPublisher;
    @Mock private ClearingMetrics clearingMetrics;
    @Mock private ClearingAlertService clearingAlertService;

    private TradeExecutedConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new TradeExecutedConsumer(settlementTaskApplicationService, tradeSettlementApplicationService,
                retryPublisher, clearingMetrics, clearingAlertService);
        lenient().when(settlementTaskApplicationService.scheduleRetry(anyString(), anyString(), anyString()))
                .thenReturn(new SettlementTaskApplicationService.RetryScheduleResult(false, 0, false));
    }

    @Test
    void shouldCreateSettlementTaskForValidTrade() {
        TradeEvent event = tradeEvent();
        when(settlementTaskApplicationService.register(event))
                .thenReturn(new SettlementTaskApplicationService.RegistrationResult(true, null));

        consumer.onTradeExecuted(event);

        verify(settlementTaskApplicationService).register(event);
        verify(tradeSettlementApplicationService).settle(event);
        verify(retryPublisher, never()).publishRetry(any(), anyInt(), any());
        verify(retryPublisher, never()).publishDlq(any(), any());
    }

    @Test
    void shouldPublishRetryForRetryableFailure() {
        TradeEvent event = tradeEvent();
        when(settlementTaskApplicationService.register(event))
                .thenThrow(new SettlementException("DATABASE_TIMEOUT", "数据库超时", true));

        consumer.onTradeExecuted(event);

        verify(retryPublisher).publishRetry(event, 1, "DATABASE_TIMEOUT");
        verify(retryPublisher, never()).publishDlq(any(), any());
    }

    @Test
    void shouldNotSettleTaskThatWasAlreadySuccessful() {
        TradeEvent event = tradeEvent();
        when(settlementTaskApplicationService.register(event))
                .thenReturn(new SettlementTaskApplicationService.RegistrationResult(false, "SUCCESS"));

        consumer.onTradeExecuted(event);

        verify(tradeSettlementApplicationService, never()).settle(event);
    }

    @Test
    void shouldPublishDlqAndMarkManualReviewForPermanentFailure() {
        TradeEvent event = tradeEvent();
        when(settlementTaskApplicationService.register(event))
                .thenThrow(new SettlementException("INVALID_TRADE", "成交字段非法", false));

        consumer.onTradeExecuted(event);

        verify(settlementTaskApplicationService).markManualReview(eq("T-1"), eq("INVALID_TRADE"), any());
        verify(retryPublisher).publishDlq(event, "INVALID_TRADE");
        verify(retryPublisher, never()).publishRetry(any(), anyInt(), any());
    }

    @Test
    void shouldPublishDlqWhenRetryCountExhausted() {
        TradeEvent event = tradeEvent();
        when(settlementTaskApplicationService.register(event))
                .thenThrow(new SettlementException("DATABASE_TIMEOUT", "数据库超时", true));

        consumer.onTradeRetry(event, 5);

        verify(settlementTaskApplicationService).markManualReview(eq("T-1"), eq("DATABASE_TIMEOUT"), any());
        verify(retryPublisher).publishDlq(event, "DATABASE_TIMEOUT");
        verify(retryPublisher, never()).publishRetry(any(), anyInt(), any());
    }

    @Test
    void shouldPersistRetryInsteadOfImmediatelyRepublishingKafkaEventWhenTaskExists() {
        TradeEvent event = tradeEvent();
        when(settlementTaskApplicationService.register(event))
                .thenReturn(new SettlementTaskApplicationService.RegistrationResult(true, null));
        org.mockito.Mockito.doThrow(new SettlementException("DATABASE_TIMEOUT", "数据库超时", true))
                .when(tradeSettlementApplicationService).settle(event);
        when(settlementTaskApplicationService.scheduleRetry("T-1", "DATABASE_TIMEOUT", "数据库超时"))
                .thenReturn(new SettlementTaskApplicationService.RetryScheduleResult(true, 1, false));

        consumer.onTradeExecuted(event);

        verify(settlementTaskApplicationService).scheduleRetry("T-1", "DATABASE_TIMEOUT", "数据库超时");
        verify(retryPublisher, never()).publishRetry(any(), anyInt(), any());
        verify(retryPublisher, never()).publishDlq(any(), any());
    }

    private TradeEvent tradeEvent() {
        return TradeEvent.builder().eventId("event-1").tradeId("T-1").sequence(1L).symbol("BTC_USDT")
                .buyOrderId("B-1").sellOrderId("S-1").buyerUserId(100L).sellerUserId(200L)
                .baseAsset("BTC").quoteAsset("USDT").price(new BigDecimal("100000"))
                .quantity(new BigDecimal("0.1")).amount(new BigDecimal("10000"))
                .buyerFee(BigDecimal.ZERO).buyerFeeAsset("BTC")
                .sellerFee(BigDecimal.ZERO).sellerFeeAsset("USDT").timestamp(System.currentTimeMillis()).build();
    }
}
