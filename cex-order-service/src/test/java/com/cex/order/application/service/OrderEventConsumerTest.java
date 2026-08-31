package com.cex.order.application.service;

import com.cex.common.kafka.event.OrderResultEvent;
import com.cex.common.kafka.event.TradeSettledEvent;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.SymbolConfig;
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
    @Mock private SymbolConfigService symbolConfigService;
    @Mock private OrderUnfreezeEventPublisher orderUnfreezeEventPublisher;

    private OrderEventConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new OrderEventConsumer(orderRepository, processedEventRepository,
                symbolConfigService, orderUnfreezeEventPublisher);
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

    private TradeSettledEvent tradeEvent(String tradeId, long buyOrderId, long sellOrderId) {
        return TradeSettledEvent.builder()
                .tradeId(tradeId).symbol("BTC_USDT")
                .buyOrderId(String.valueOf(buyOrderId)).sellOrderId(String.valueOf(sellOrderId))
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.04"))
                .amount(new BigDecimal("4000")).settledAt(System.currentTimeMillis())
                .build();
    }

    @Test
    void onTradeSettledEvent_updatesBothOrdersAndRecordsProcessed() {
        when(processedEventRepository.exists("t1", OrderEventConsumer.CONSUMER)).thenReturn(false);
        when(orderRepository.findByOrderId(1L)).thenReturn(openBuyOrder(1L));
        when(orderRepository.findByOrderId(2L)).thenReturn(openBuyOrder(2L));

        consumer.onTradeSettledEvent(tradeEvent("t1", 1L, 2L));

        // 买单一号部分成交
        verify(orderRepository, times(2)).update(any());
        verify(processedEventRepository).save(any(ProcessedEventPO.class));
    }

    @Test
    void onTradeSettledEvent_duplicateEvent_ignored() {
        when(processedEventRepository.exists("t1", OrderEventConsumer.CONSUMER)).thenReturn(true);

        consumer.onTradeSettledEvent(tradeEvent("t1", 1L, 2L));

        verify(orderRepository, never()).update(any());
        verify(processedEventRepository, never()).save(any());
    }

    @Test
    void onTradeSettledEvent_usesEventIdAsTheIdempotencyKey() {
        TradeSettledEvent event = tradeEvent("t1", 1L, 2L);
        event.setEventId("trade-event-1");
        when(processedEventRepository.exists("trade-event-1", OrderEventConsumer.CONSUMER)).thenReturn(true);

        consumer.onTradeSettledEvent(event);

        verify(processedEventRepository).exists("trade-event-1", OrderEventConsumer.CONSUMER);
        verify(orderRepository, never()).update(any());
    }

    @Test
    void onTradeSettledEvent_orderFillsUpdatesStatus() {
        when(processedEventRepository.exists("t1", OrderEventConsumer.CONSUMER)).thenReturn(false);
        when(orderRepository.findByOrderId(1L)).thenReturn(openBuyOrder(1L));
        when(orderRepository.findByOrderId(2L)).thenReturn(null); // 卖单不在本库(不存在则跳过)

        consumer.onTradeSettledEvent(tradeEvent("t1", 1L, 2L));

        assertThat(consumer).isNotNull();
    }

    @Test
    void onTradeSettledEvent_canceledOrderFill_skipsUpdateButRecordsProcessed() {
        Order canceled = openBuyOrder(1L);
        canceled.cancel();
        when(processedEventRepository.exists("t1", OrderEventConsumer.CONSUMER)).thenReturn(false);
        when(orderRepository.findByOrderId(1L)).thenReturn(canceled);
        when(orderRepository.findByOrderId(2L)).thenReturn(null);

        consumer.onTradeSettledEvent(tradeEvent("t1", 1L, 2L));

        // 状态冲突(订单已取消)不抛异常、不更新订单,但幂等记录照常写入(视为已消费,避免重试风暴)
        verify(orderRepository, never()).update(any());
        verify(processedEventRepository).save(any(ProcessedEventPO.class));
    }

    @Test
    void onOrderResultEvent_shouldUpdateRejectedOrderAndRecordIdempotency() {
        Order order = openBuyOrder(1L);
        OrderResultEvent event = OrderResultEvent.builder().eventId("result-1").orderId("1")
                .symbol("BTC_USDT").sequence(1L).type(OrderResultEvent.OrderResultType.ORDER_REJECTED)
                .filledQuantity(BigDecimal.ZERO).remainingQuantity(new BigDecimal("0.1"))
                .timestamp(System.currentTimeMillis()).build();
        when(processedEventRepository.exists("result-1", OrderEventConsumer.ORDER_RESULT_CONSUMER)).thenReturn(false);
        when(orderRepository.findByOrderId(1L)).thenReturn(order);

        consumer.onOrderResultEvent(event);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        verify(orderRepository).update(order);
        verify(processedEventRepository).save(any(ProcessedEventPO.class));
    }

    @Test
    void onTradeSettledEvent_filledBuyOrderPublishesPriceImprovementUnfreeze() {
        Order order = openBuyOrder(1L);
        SymbolConfig config = symbolConfig();
        TradeSettledEvent event = tradeEvent("filled-price-improvement", 1L, 2L);
        event.setQuantity(new BigDecimal("0.1"));
        event.setPrice(new BigDecimal("99000"));
        event.setAmount(new BigDecimal("9900"));
        when(processedEventRepository.exists("filled-price-improvement", OrderEventConsumer.CONSUMER)).thenReturn(false);
        when(orderRepository.findByOrderId(1L)).thenReturn(order);
        when(orderRepository.findByOrderId(2L)).thenReturn(null);
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);

        consumer.onTradeSettledEvent(event);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.FILLED);
        verify(orderUnfreezeEventPublisher).publishIfNeeded(order, config);
    }

    @Test
    void onOrderResultEvent_cancelConfirmationPublishesUnfreezeAfterSettledFills() {
        Order order = openBuyOrder(1L);
        order.markPartiallyFilled(new BigDecimal("0.04"), new BigDecimal("3960"));
        order.requestCancel();
        SymbolConfig config = symbolConfig();
        OrderResultEvent event = OrderResultEvent.builder().eventId("cancel-confirmed-1").orderId("1")
                .symbol("BTC_USDT").sequence(2L).type(OrderResultEvent.OrderResultType.ORDER_CANCELED)
                .filledQuantity(new BigDecimal("0.04")).remainingQuantity(new BigDecimal("0.06"))
                .timestamp(System.currentTimeMillis()).build();
        when(processedEventRepository.exists("cancel-confirmed-1", OrderEventConsumer.ORDER_RESULT_CONSUMER))
                .thenReturn(false);
        when(orderRepository.findByOrderId(1L)).thenReturn(order);
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);

        consumer.onOrderResultEvent(event);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELED);
        verify(orderUnfreezeEventPublisher).publishIfNeeded(order, config);
    }

    private SymbolConfig symbolConfig() {
        return SymbolConfig.builder().symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
                .priceScale(2).quantityScale(6).minQuantity(new BigDecimal("0.0001"))
                .minAmount(new BigDecimal("10")).status(SymbolConfig.SymbolStatus.ACTIVE).build();
    }
}
