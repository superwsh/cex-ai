package com.cex.order.application.service;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.common.kafka.event.OrderUnfreezeEvent;
import com.cex.order.infrastructure.kafka.OrderKafkaProducer;
import com.cex.order.infrastructure.kafka.OrderUnfreezeKafkaProducer;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import com.cex.order.infrastructure.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxRelayServiceTest {

    @Mock private OutboxRepository outboxRepository;
    @Mock private OrderKafkaProducer kafkaProducer;
    @Mock private OrderUnfreezeKafkaProducer orderUnfreezeKafkaProducer;

    private OutboxRelayService service;

    @BeforeEach
    void setUp() {
        service = new OutboxRelayService(outboxRepository, kafkaProducer, orderUnfreezeKafkaProducer, new ObjectMapper());
    }

    private OrderEventOutboxPO pendingOutbox() {
        return OrderEventOutboxPO.builder()
                .id(1L).eventId("evt-1").aggregateType("ORDER").aggregateId("1")
                .eventType(OrderEventPublisher.EVENT_ORDER_CREATED)
                .payload("{\"orderId\":\"1\",\"symbol\":\"BTC_USDT\",\"quantity\":0.1}")
                .status(OrderEventOutboxPO.STATUS_INIT).retryCount(0)
                .nextRetryTime(LocalDateTime.now()).createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }

    @Test
    void relay_success_marksSuccess() {
        when(outboxRepository.findPending(100, 10)).thenReturn(List.of(pendingOutbox()));

        service.relay();

        ArgumentCaptor<OrderEventOutboxPO> captor = ArgumentCaptor.forClass(OrderEventOutboxPO.class);
        // 两次更新:先置 SENDING,成功后置 SUCCESS;captor.getValue() 取最后一次
        verify(outboxRepository, org.mockito.Mockito.times(2)).update(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(OrderEventOutboxPO.STATUS_SUCCESS);
        verify(kafkaProducer).send(any());
    }

    @Test
    void relay_sendFails_retryCountIncremented() {
        OrderEventOutboxPO outbox = pendingOutbox();
        when(outboxRepository.findPending(100, 10)).thenReturn(List.of(outbox));
        doThrow(new RuntimeException("kafka down")).when(kafkaProducer).send(any());

        service.relay();

        // 两次更新:一次置 SENDING,一次失败重试(回到 INIT 等待退避)
        verify(outboxRepository, org.mockito.Mockito.times(2)).update(any());
        assertThat(outbox.getStatus()).isEqualTo(OrderEventOutboxPO.STATUS_INIT);
        assertThat(outbox.getRetryCount()).isEqualTo(1);
    }

    @Test
    void relay_retryExceeded_marksFailed() {
        OrderEventOutboxPO outbox = pendingOutbox();
        outbox.setRetryCount(9); // 达到上限(10)前最后一次
        when(outboxRepository.findPending(100, 10)).thenReturn(List.of(outbox));
        doThrow(new RuntimeException("kafka down")).when(kafkaProducer).send(any());

        service.relay();

        // 两次更新:一次置 SENDING,一次失败置 FAILED;对象原地变更,直接断言最终状态
        verify(outboxRepository, org.mockito.Mockito.times(2)).update(any());
        assertThat(outbox.getStatus()).isEqualTo(OrderEventOutboxPO.STATUS_FAILED);
    }

    @Test
    void relay_orderUnfreezeEvent_routesToClearingTopicProducer() throws Exception {
        OrderEventOutboxPO outbox = pendingOutbox();
        outbox.setEventType(OrderUnfreezeEventPublisher.EVENT_ORDER_UNFREEZE);
        outbox.setPayload(new ObjectMapper().writeValueAsString(OrderUnfreezeEvent.builder().eventId("unfreeze-1")
                .orderId(1L).userId(100L).asset("USDT").amount(new BigDecimal("100")).reason("FILLED")
                .timestamp(System.currentTimeMillis()).build()));
        when(outboxRepository.findPending(100, 10)).thenReturn(List.of(outbox));

        service.relay();

        verify(orderUnfreezeKafkaProducer).send(any(OrderUnfreezeEvent.class));
    }
}
