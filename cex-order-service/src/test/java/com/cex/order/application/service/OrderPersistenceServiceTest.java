package com.cex.order.application.service;

import com.cex.order.application.command.CreateOrderCommand;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderFactory;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.SymbolConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OrderPersistenceServiceTest {

    @Mock private OrderRepository orderRepository;
    @Mock private OrderEventPublisher eventPublisher;

    private OrderPersistenceService service;

    private final SymbolConfig config = SymbolConfig.builder()
            .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
            .priceScale(2).quantityScale(6)
            .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
            .status(SymbolConfig.SymbolStatus.ACTIVE)
            .build();

    @BeforeEach
    void setUp() {
        service = new OrderPersistenceService(orderRepository, new OrderFactory(), eventPublisher);
    }

    @Test
    void createOrderInTx_marketBuy_persistsZeroQuantityAndKeepsQuoteAmount() {
        CreateOrderCommand command = CreateOrderCommand.builder()
                .userId(100L).clientOrderId("c_mkt_buy").symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.MARKET)
                .quoteAmount(new BigDecimal("5000"))
                .timeInForce(TimeInForce.GTC)
                .build();

        service.createOrderInTx(command, 1L, config);

        // 市价买单 quantity 恒为 ZERO(避免 MySQL NOT NULL 插入失败),quoteAmount 原样保留
        ArgumentCaptor<Order> captor = ArgumentCaptor.forClass(Order.class);
        verify(orderRepository).insert(captor.capture());
        Order order = captor.getValue();
        assertThat(order.getQuantity()).isEqualByComparingTo("0");
        assertThat(order.getQuoteAmount()).isEqualByComparingTo("5000");
        assertThat(order.getPrice()).isNull();
    }
}
