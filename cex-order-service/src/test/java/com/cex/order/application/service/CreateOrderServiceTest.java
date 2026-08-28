package com.cex.order.application.service;

import com.baomidou.mybatisplus.core.exceptions.MybatisPlusException;
import com.cex.common.core.exception.BizException;
import com.cex.order.application.command.CreateOrderCommand;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.FreezeCalculator;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.domain.service.TradingRuleValidator;
import com.cex.order.infrastructure.asset.AccountServiceClient;
import com.cex.order.infrastructure.asset.FreezeRequest;
import com.cex.order.infrastructure.asset.UnfreezeRequest;
import com.cex.order.infrastructure.id.SnowflakeGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CreateOrderServiceTest {

    @Mock private OrderRepository orderRepository;
    @Mock private SymbolConfigService symbolConfigService;
    @Mock private AccountServiceClient accountServiceClient;
    @Mock private OrderPersistenceService persistenceService;

    private CreateOrderService service;

    private final SymbolConfig config = SymbolConfig.builder()
            .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
            .priceScale(2).quantityScale(6)
            .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
            .status(SymbolConfig.SymbolStatus.ACTIVE)
            .build();

    private final SnowflakeGenerator snowflake = new SnowflakeGenerator(1, 1);

    @BeforeEach
    void setUp() {
        service = new CreateOrderService(orderRepository, symbolConfigService,
                new TradingRuleValidator(), new FreezeCalculator(),
                accountServiceClient, persistenceService, snowflake);
    }

    private CreateOrderCommand limitBuyCommand() {
        return CreateOrderCommand.builder()
                .userId(100L).clientOrderId("client_order_123").symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .timeInForce(TimeInForce.GTC)
                .build();
    }

    @Test
    void createOrder_success_freezeThenPersist() {
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);
        when(persistenceService.createOrderInTx(any(), anyLong(), any())).thenReturn(mock(CreateOrderResult.class));

        service.createOrder(limitBuyCommand());

        ArgumentCaptor<FreezeRequest> freezeCaptor = ArgumentCaptor.forClass(FreezeRequest.class);
        verify(accountServiceClient).freeze(freezeCaptor.capture());
        FreezeRequest freeze = freezeCaptor.getValue();
        assertThat(freeze.getUserId()).isEqualTo(100L);
        assertThat(freeze.getCurrency()).isEqualTo("USDT");
        assertThat(freeze.getAmount()).isEqualByComparingTo("10000"); // 100000 * 0.1
        assertThat(freeze.getBizType()).isEqualTo("FREEZE_ORDER");
        assertThat(freeze.getBizId()).isNotNull();
        verify(persistenceService).createOrderInTx(any(), anyLong(), any());
    }

    @Test
    void createOrder_duplicateClientOrderId_returnsExistingWithoutFreeze() {
        Order existing = Order.builder().orderId(1L).userId(100L)
                .clientOrderId("client_order_123").symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .status(com.cex.order.domain.model.OrderStatus.PENDING_MATCH)
                .build();
        when(orderRepository.findByUserIdAndClientOrderId(100L, "client_order_123"))
                .thenReturn(existing);

        CreateOrderResult result = service.createOrder(limitBuyCommand());

        assertThat(result.getOrderId()).isEqualTo(1L);
        verify(accountServiceClient, never()).freeze(any());
        verify(persistenceService, never()).createOrderInTx(any(), anyLong(), any());
    }

    @Test
    void createOrder_symbolNotExist_throwsBeforeFreeze() {
        when(symbolConfigService.getRequired("BTC_USDT"))
                .thenThrow(new BizException(40010, "交易对不存在: BTC_USDT"));

        assertThatThrownBy(() -> service.createOrder(limitBuyCommand()))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("交易对不存在");
        verify(accountServiceClient, never()).freeze(any());
    }

    @Test
    void createOrder_freezerFails_throwsAndNoPersist() {
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);
        doThrow(new BizException(50011, "余额不足"))
                .when(accountServiceClient).freeze(any());

        assertThatThrownBy(() -> service.createOrder(limitBuyCommand()))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("余额不足");
        verify(persistenceService, never()).createOrderInTx(any(), anyLong(), any());
    }

    @Test
    void createOrder_persistFails_unfreezeCompensates() {
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);
        when(persistenceService.createOrderInTx(any(), anyLong(), any()))
                .thenThrow(new RuntimeException("db down"));

        assertThatThrownBy(() -> service.createOrder(limitBuyCommand()))
                .isInstanceOf(RuntimeException.class);

        ArgumentCaptor<FreezeRequest> freezeCaptor = ArgumentCaptor.forClass(FreezeRequest.class);
        verify(accountServiceClient).freeze(freezeCaptor.capture());
        ArgumentCaptor<UnfreezeRequest> unfreezeCaptor = ArgumentCaptor.forClass(UnfreezeRequest.class);
        verify(accountServiceClient).unfreeze(unfreezeCaptor.capture());

        assertThat(unfreezeCaptor.getValue().getBizId())
                .isEqualTo(freezeCaptor.getValue().getBizId());
        assertThat(unfreezeCaptor.getValue().getAmount()).isEqualByComparingTo("10000");
    }

    @Test
    void createOrder_priceScaleError_throwsBeforeFreeze() {
        CreateOrderCommand bad = limitBuyCommand();
        bad.setPrice(new BigDecimal("100000.123")); // 3 位小数,超出 priceScale=2
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);

        assertThatThrownBy(() -> service.createOrder(bad))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("价格精度");
        verify(accountServiceClient, never()).freeze(any());
    }

    @Test
    void createOrder_marketBuy_freezesQuoteAmount() {
        CreateOrderCommand marketBuy = CreateOrderCommand.builder()
                .userId(100L).clientOrderId("c_mkt").symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.MARKET)
                .quoteAmount(new BigDecimal("5000"))
                .timeInForce(TimeInForce.GTC)
                .build();
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);
        when(persistenceService.createOrderInTx(any(), anyLong(), any())).thenReturn(mock(CreateOrderResult.class));

        service.createOrder(marketBuy);

        ArgumentCaptor<FreezeRequest> captor = ArgumentCaptor.forClass(FreezeRequest.class);
        verify(accountServiceClient).freeze(captor.capture());
        assertThat(captor.getValue().getAmount()).isEqualByComparingTo("5000");
    }
}
