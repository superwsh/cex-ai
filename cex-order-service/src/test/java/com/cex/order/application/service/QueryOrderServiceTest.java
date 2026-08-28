package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.api.response.OrderResponse;
import com.cex.order.api.response.PageResult;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import com.cex.order.domain.repository.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueryOrderServiceTest {

    @Mock private OrderRepository orderRepository;

    private QueryOrderService service;

    @BeforeEach
    void setUp() {
        service = new QueryOrderService(orderRepository);
    }

    private Order order(long id) {
        return Order.builder()
                .id(id).orderId(id).userId(100L).clientOrderId("c" + id)
                .symbol("BTC_USDT").side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .filledQuantity(BigDecimal.ZERO).filledAmount(BigDecimal.ZERO)
                .status(OrderStatus.PENDING_MATCH).timeInForce(TimeInForce.GTC)
                .createdAt(LocalDateTime.now().minusMinutes(id))
                .build();
    }

    @Test
    void getOrder_success() {
        when(orderRepository.findByOrderId(1L)).thenReturn(order(1L));
        OrderResponse response = service.getOrder(100L, 1L);
        assertThat(response.getOrderId()).isEqualTo(1L);
        assertThat(response.getStatus()).isEqualTo(OrderStatus.PENDING_MATCH);
    }

    @Test
    void getOrder_wrongUser_throws() {
        when(orderRepository.findByOrderId(1L)).thenReturn(order(1L));
        assertThatThrownBy(() -> service.getOrder(999L, 1L))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("订单不存在");
    }

    @Test
    void listOpenOrders_passesCursor() {
        when(orderRepository.listOpenOrders(eq(100L), eq("BTC_USDT"), eq(20),
                any(), any())).thenReturn(LongStream.rangeClosed(1, 20)
                .mapToObj(this::order).toList());

        PageResult<OrderResponse> result = service.listOpenOrders(100L, "BTC_USDT", null, null);

        assertThat(result.getItems()).hasSize(20);
        assertThat(result.getNextCursor()).isNotBlank().contains("_");
    }

    @Test
    void listHistoryOrders_returnsPage() {
        when(orderRepository.listHistoryOrders(eq(100L), eq("BTC_USDT"), eq(20),
                any(), any())).thenReturn(List.of(order(3L)));

        PageResult<OrderResponse> result = service.listHistoryOrders(100L, "BTC_USDT", null, null);

        assertThat(result.getItems()).hasSize(1);
    }
}
