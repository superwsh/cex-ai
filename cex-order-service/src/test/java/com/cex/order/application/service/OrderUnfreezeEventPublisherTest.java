package com.cex.order.application.service;

import com.cex.common.kafka.event.OrderUnfreezeEvent;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.service.FreezeCalculator;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.id.SnowflakeGenerator;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import com.cex.order.infrastructure.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 订单终态解冻 Outbox 载荷测试。 */
@ExtendWith(MockitoExtension.class)
class OrderUnfreezeEventPublisherTest {

    @Mock private OutboxRepository outboxRepository;
    @Mock private SnowflakeGenerator snowflakeGenerator;

    @Test
    void shouldPublishOnlyPriceImprovementDifferenceForFilledLimitBuy() throws Exception {
        when(snowflakeGenerator.nextId()).thenReturn(1L);
        OrderUnfreezeEventPublisher publisher = new OrderUnfreezeEventPublisher(outboxRepository,
                snowflakeGenerator, new ObjectMapper(), new FreezeCalculator());
        Order order = Order.builder().orderId(1L).userId(100L).symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.LIMIT).price(new BigDecimal("100000"))
                .quantity(new BigDecimal("0.1")).filledQuantity(new BigDecimal("0.1"))
                .filledAmount(new BigDecimal("9900")).status(OrderStatus.FILLED).build();
        SymbolConfig config = SymbolConfig.builder().symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
                .priceScale(2).quantityScale(6).minQuantity(new BigDecimal("0.0001"))
                .minAmount(new BigDecimal("10")).status(SymbolConfig.SymbolStatus.ACTIVE).build();

        publisher.publishIfNeeded(order, config);

        ArgumentCaptor<OrderEventOutboxPO> captor = ArgumentCaptor.forClass(OrderEventOutboxPO.class);
        verify(outboxRepository).insert(captor.capture());
        OrderUnfreezeEvent event = new ObjectMapper().readValue(captor.getValue().getPayload(), OrderUnfreezeEvent.class);
        assertThat(event.getAmount()).isEqualByComparingTo("100");
        assertThat(event.getAsset()).isEqualTo("USDT");
    }
}
