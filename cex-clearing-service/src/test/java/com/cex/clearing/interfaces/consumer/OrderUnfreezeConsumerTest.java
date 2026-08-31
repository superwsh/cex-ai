package com.cex.clearing.interfaces.consumer;

import com.cex.clearing.application.service.AccountCommandApplicationService;
import com.cex.common.dubbo.account.UnfreezeBalanceRequest;
import com.cex.common.kafka.event.OrderUnfreezeEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

/** 订单终态解冻事件消费测试。 */
@ExtendWith(MockitoExtension.class)
class OrderUnfreezeConsumerTest {

    @Mock private AccountCommandApplicationService accountCommandApplicationService;

    @Test
    void shouldForwardTerminalOrderUnfreezeToIdempotentAccountCommand() {
        OrderUnfreezeConsumer consumer = new OrderUnfreezeConsumer(accountCommandApplicationService);
        OrderUnfreezeEvent event = OrderUnfreezeEvent.builder().eventId("order-unfreeze-1").orderId(1L)
                .userId(100L).asset("USDT").amount(new BigDecimal("100")).reason("FILLED")
                .timestamp(System.currentTimeMillis()).build();

        consumer.onOrderUnfreeze(event);

        ArgumentCaptor<UnfreezeBalanceRequest> captor = ArgumentCaptor.forClass(UnfreezeBalanceRequest.class);
        verify(accountCommandApplicationService).unfreeze(captor.capture());
        assertThat(captor.getValue().bizId()).isEqualTo("1");
        assertThat(captor.getValue().amount()).isEqualByComparingTo("100");
    }
}
