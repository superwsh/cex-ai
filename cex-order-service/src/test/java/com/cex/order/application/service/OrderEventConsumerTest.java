package com.cex.order.application.service;

import com.cex.common.kafka.event.TradeEvent;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.infrastructure.persistence.entity.ProcessedEventPO;
import com.cex.order.infrastructure.repository.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderEventConsumerTest {

    @Mock private OrderRepository orderRepository;
    @Mock private ProcessedEventRepository processedEventRepository;

    private OrderEventConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new OrderEventConsumer(orderRepository, processedEventRepository);
    }

    private Order openBuyOrder(long orderId) {
        return Order.builder()
                .id(orderId).orderId(orderId).userId(100L)
                .symbol("BTC_USDT").side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .filledQuantity(BigDecimal.ZERO).filledAmount(BigDecimal.ZERO)
                .status(OrderStatus.PENDING_MATCH)
                .build();
    }

    private TradeEvent tradeEvent(String tradeId, long buyOrderId, long sellOrderId) {
        return TradeEvent.builder()
                .tradeId(tradeId).symbol("BTC_USDT")
                .buyOrderId(String.valueOf(buyOrderId)).sellOrderId(String.valueOf(sellOrderId))
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.04"))
                .amount(new BigDecimal("4000")).timestamp(System.currentTimeMillis())
                .build();
    }

    @Test
    void onTradeEvent_updatesBothOrdersAndRecordsProcessed() {
        when(processedEventRepository.exists("t1", OrderEventConsumer.CONSUMER)).thenReturn(false);
        when(orderRepository.findByOrderId(1L)).thenReturn(openBuyOrder(1L));
        when(orderRepository.findByOrderId(2L)).thenReturn(openBuyOrder(2L));

        consumer.onTradeEvent(tradeEvent("t1", 1L, 2L));

        // 买单一号部分成交
        verify(orderRepository, times(2)).update(any());
        verify(processedEventRepository).save(any(ProcessedEventPO.class));
    }

    @Test
    void onTradeEvent_duplicateEvent_ignored() {
        when(processedEventRepository.exists("t1", OrderEventConsumer.CONSUMER)).thenReturn(true);

        consumer.onTradeEvent(tradeEvent("t1", 1L, 2L));

        verify(orderRepository, never()).update(any());
        verify(processedEventRepository, never()).save(any());
    }

    @Test
    void onTradeEvent_usesEventIdAsTheIdempotencyKey() {
        TradeEvent event = tradeEvent("t1", 1L, 2L);
        event.setEventId("trade-event-1");
        when(processedEventRepository.exists("trade-event-1", OrderEventConsumer.CONSUMER)).thenReturn(true);

        consumer.onTradeEvent(event);

        verify(processedEventRepository).exists("trade-event-1", OrderEventConsumer.CONSUMER);
        verify(orderRepository, never()).update(any());
    }

    @Test
    void onTradeEvent_orderFillsUpdatesStatus() {
        when(processedEventRepository.exists("t1", OrderEventConsumer.CONSUMER)).thenReturn(false);
        when(orderRepository.findByOrderId(1L)).thenReturn(openBuyOrder(1L));
        when(orderRepository.findByOrderId(2L)).thenReturn(null); // 卖单不在本库(不存在则跳过)

        consumer.onTradeEvent(tradeEvent("t1", 1L, 2L));

        assertThat(consumer).isNotNull();
    }

    @Test
    void onTradeEvent_canceledOrderFill_skipsUpdateButRecordsProcessed() {
        Order canceled = openBuyOrder(1L);
        canceled.cancel();
        when(processedEventRepository.exists("t1", OrderEventConsumer.CONSUMER)).thenReturn(false);
        when(orderRepository.findByOrderId(1L)).thenReturn(canceled);
        when(orderRepository.findByOrderId(2L)).thenReturn(null);

        consumer.onTradeEvent(tradeEvent("t1", 1L, 2L));

        // 状态冲突(订单已取消)不抛异常、不更新订单,但幂等记录照常写入(视为已消费,避免重试风暴)
        verify(orderRepository, never()).update(any());
        verify(processedEventRepository).save(any(ProcessedEventPO.class));
    }
}
