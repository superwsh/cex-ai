package com.cex.order.application.service;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.id.SnowflakeGenerator;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import com.cex.order.infrastructure.repository.MatchingCommandSequenceRepository;
import com.cex.order.infrastructure.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderEventPublisherTest {

    @Mock private OutboxRepository outboxRepository;
    @Mock private MatchingCommandSequenceRepository matchingCommandSequenceRepository;
    @Mock private SnowflakeGenerator snowflakeGenerator;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private OrderEventPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new OrderEventPublisher(outboxRepository, matchingCommandSequenceRepository,
                snowflakeGenerator, objectMapper);
    }

    @Test
    void publishOrderCreated_shouldPersistAllocatedSymbolSequenceInEventPayload() throws Exception {
        Order order = order();
        when(matchingCommandSequenceRepository.allocateNext("BTC_USDT")).thenReturn(42L);
        when(snowflakeGenerator.nextId()).thenReturn(99L);

        publisher.publishOrderCreated(order, symbolConfig());

        ArgumentCaptor<OrderEventOutboxPO> captor = ArgumentCaptor.forClass(OrderEventOutboxPO.class);
        verify(outboxRepository).insert(captor.capture());
        OrderEvent event = objectMapper.readValue(captor.getValue().getPayload(), OrderEvent.class);
        assertThat(event.getSequence()).isEqualTo(42L);
        verify(matchingCommandSequenceRepository).allocateNext("BTC_USDT");
        assertThat(event.getBaseAsset()).isEqualTo("BTC");
        assertThat(event.getQuoteAsset()).isEqualTo("USDT");
    }

    /** 创建用于验证 Outbox 事件载荷的有效限价订单。 */
    private Order order() {
        return Order.builder()
                .id(1L).orderId(1L).userId(7L).clientOrderId("client-1").symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.LIMIT).price(new BigDecimal("100"))
                .quantity(BigDecimal.ONE).filledQuantity(BigDecimal.ZERO).filledAmount(BigDecimal.ZERO)
                .status(OrderStatus.PENDING_MATCH).timeInForce(TimeInForce.GTC)
                .build();
    }

    private SymbolConfig symbolConfig() {
        return SymbolConfig.builder().symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
                .priceScale(2).quantityScale(6).minQuantity(new BigDecimal("0.0001"))
                .minAmount(BigDecimal.TEN).status(SymbolConfig.SymbolStatus.ACTIVE).build();
    }
}
