package com.cex.clearing.application.service;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.cex.clearing.infrastructure.persistence.entity.SettlementEventOutboxPO;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementEventOutboxMapper;
import com.cex.clearing.application.observability.ClearingAlertService;
import com.cex.clearing.application.observability.ClearingMetrics;
import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.TradeSettledEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 结算 Outbox 可靠发布、重试与多实例原子认领测试。 */
@ExtendWith(MockitoExtension.class)
class SettlementOutboxRelayServiceTest {

    @Mock
    private SettlementEventOutboxMapper outboxMapper;
    @Mock
    private KafkaTemplate<String, TradeSettledEvent> kafkaTemplate;
    @Mock private ClearingMetrics clearingMetrics;
    @Mock private ClearingAlertService clearingAlertService;

    private SettlementOutboxRelayService relayService;

    @BeforeEach
    void setUp() {
        relayService = new SettlementOutboxRelayService(outboxMapper, kafkaTemplate, new ObjectMapper(),
                clearingMetrics, clearingAlertService);
    }

    /** 正常发送成功后必须以持有的处理令牌标记 PUBLISHED。 */
    @Test
    void shouldMarkPublishedWhenKafkaSendSucceeds() throws Exception {
        SettlementEventOutboxPO record = record(0);
        stubPendingRecord(record);
        CompletableFuture<SendResult<String, TradeSettledEvent>> sent = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(eq(TopicConstants.TOPIC_TRADE_SETTLED_EVENT), eq("BTC-USDT"), any())).thenReturn(sent);
        when(outboxMapper.markPublished(eq(1L), anyString(), any(LocalDateTime.class))).thenReturn(1);

        relayService.relay();

        verify(kafkaTemplate).send(eq(TopicConstants.TOPIC_TRADE_SETTLED_EVENT), eq("BTC-USDT"), any());
        verify(outboxMapper).markPublished(eq(1L), anyString(), any(LocalDateTime.class));
        verify(outboxMapper, never()).markRetry(anyLong(), anyString(), anyString(), anyInt(), any(), anyString(), any());
    }

    /** Kafka 发送失败后必须保留事件并按指数退避进入 RETRY。 */
    @Test
    void shouldScheduleRetryWhenKafkaSendFails() throws Exception {
        SettlementEventOutboxPO record = record(0);
        stubPendingRecord(record);
        CompletableFuture<SendResult<String, TradeSettledEvent>> failed = CompletableFuture.failedFuture(new RuntimeException("broker down"));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(failed);

        relayService.relay();

        verify(outboxMapper).markRetry(eq(1L), anyString(), eq("RETRY"), eq(1),
                any(LocalDateTime.class), ArgumentMatchers.contains("RuntimeException"), any(LocalDateTime.class));
        verify(outboxMapper, never()).markPublished(anyLong(), anyString(), any());
    }

    /** 最后一次发送失败必须进入 FAILED，避免无限重试。 */
    @Test
    void shouldMarkFailedWhenRetryLimitReached() throws Exception {
        SettlementEventOutboxPO record = record(9);
        stubPendingRecord(record);
        CompletableFuture<SendResult<String, TradeSettledEvent>> failed = CompletableFuture.failedFuture(new RuntimeException("broker down"));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(failed);

        relayService.relay();

        verify(outboxMapper).markRetry(eq(1L), anyString(), eq("FAILED"), eq(10),
                any(LocalDateTime.class), anyString(), any(LocalDateTime.class));
    }

    /** 被其他实例先认领的记录不能再次发送或更新状态。 */
    @Test
    void shouldSkipRecordWhenClaimIsHeldByAnotherInstance() throws Exception {
        SettlementEventOutboxPO record = record(0);
        when(outboxMapper.selectList(any(Wrapper.class))).thenReturn(List.of(record));
        when(outboxMapper.claimForPublish(eq(1L), anyString(), any(LocalDateTime.class),
                any(LocalDateTime.class), eq(10))).thenReturn(0);

        relayService.relay();

        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
        verify(outboxMapper, never()).markPublished(anyLong(), anyString(), any());
        verify(outboxMapper, never()).markRetry(anyLong(), anyString(), anyString(), anyInt(), any(), anyString(), any());
    }

    /** 构造可发布的 trade.settled Outbox 记录。 */
    private SettlementEventOutboxPO record(int retryCount) throws Exception {
        TradeSettledEvent event = TradeSettledEvent.builder().eventId("trade-settled-T1").tradeId("T1")
                .symbol("BTC-USDT").buyOrderId("B1").sellOrderId("S1")
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .amount(new BigDecimal("10000")).settledAt(System.currentTimeMillis()).build();
        SettlementEventOutboxPO record = new SettlementEventOutboxPO();
        record.setId(1L);
        record.setEventId(event.getEventId());
        record.setTopic(TopicConstants.TOPIC_TRADE_SETTLED_EVENT);
        record.setPayload(new ObjectMapper().writeValueAsString(event));
        record.setRetryCount(retryCount);
        return record;
    }

    /** 配置当前实例成功取得发送租约。 */
    private void stubPendingRecord(SettlementEventOutboxPO record) {
        when(outboxMapper.selectList(any(Wrapper.class))).thenReturn(List.of(record));
        when(outboxMapper.claimForPublish(eq(1L), anyString(), any(LocalDateTime.class),
                any(LocalDateTime.class), eq(10))).thenReturn(1);
    }
}
