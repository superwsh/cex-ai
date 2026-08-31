package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.application.command.CancelOrderCommand;
import com.cex.order.common.OrderStatusInvalidException;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.FreezeCalculator;
import com.cex.order.domain.service.SymbolConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CancelOrderServiceTest {

    @Mock private OrderRepository orderRepository;
    @Mock private SymbolConfigService symbolConfigService;
    @Mock private OrderPersistenceService persistenceService;

    private CancelOrderService service;

    private final SymbolConfig config = SymbolConfig.builder()
            .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
            .priceScale(2).quantityScale(6)
            .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
            .status(SymbolConfig.SymbolStatus.ACTIVE)
            .build();

    @BeforeEach
    void setUp() {
        service = new CancelOrderService(orderRepository, symbolConfigService, persistenceService);
    }

    private Order openBuyOrder() {
        return Order.builder()
                .id(1L).orderId(1L).userId(100L).clientOrderId("c1")
                .symbol("BTC_USDT").side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .filledQuantity(BigDecimal.ZERO).filledAmount(BigDecimal.ZERO)
                .status(OrderStatus.PENDING_MATCH).timeInForce(TimeInForce.GTC)
                .build();
    }

    private CancelOrderCommand cancelCommand() {
        return CancelOrderCommand.builder().userId(100L).orderId(1L).build();
    }

    @Test
    void cancel_success_marksRequestedAndWaitsForMatchingConfirmation() {
        Order order = openBuyOrder();
        when(orderRepository.findByOrderId(1L)).thenReturn(order);
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);

        service.cancelOrder(cancelCommand());

        verify(persistenceService).cancelInTx(any(), any());
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCEL_REQUESTED);
    }

    @Test
    void cancel_orderNotExist_throws() {
        when(orderRepository.findByOrderId(99L)).thenReturn(null);
        CancelOrderCommand cmd = CancelOrderCommand.builder().userId(100L).orderId(99L).build();

        assertThatThrownBy(() -> service.cancelOrder(cmd))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("订单不存在");
        verify(persistenceService, never()).cancelInTx(any(), any());
    }

    @Test
    void cancel_wrongUser_throws() {
        when(orderRepository.findByOrderId(1L)).thenReturn(openBuyOrder());
        CancelOrderCommand cmd = CancelOrderCommand.builder().userId(999L).orderId(1L).build();

        assertThatThrownBy(() -> service.cancelOrder(cmd))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("订单不存在");
    }

    @Test
    void cancel_filledOrder_rejected() {
        Order filled = openBuyOrder();
        filled.markPartiallyFilled(new BigDecimal("0.1"), new BigDecimal("10000"));
        when(orderRepository.findByOrderId(1L)).thenReturn(filled);

        assertThatThrownBy(() -> service.cancelOrder(cancelCommand()))
                .isInstanceOf(OrderStatusInvalidException.class)
                .hasMessageContaining("FILLED");
        verify(persistenceService, never()).cancelInTx(any(), any());
    }

    @Test
    void cancel_partiallyFilled_waitsForMatchingConfirmation() {
        Order partial = openBuyOrder();
        partial.markPartiallyFilled(new BigDecimal("0.04"), new BigDecimal("4000"));
        when(orderRepository.findByOrderId(1L)).thenReturn(partial);
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);

        service.cancelOrder(cancelCommand());

        assertThat(partial.getStatus()).isEqualTo(OrderStatus.CANCEL_REQUESTED);
    }
}
