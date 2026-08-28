package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.redis.SymbolConfigCache;
import com.cex.order.infrastructure.repository.SymbolConfigRepository;
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
class SymbolConfigServiceTest {

    @Mock
    private SymbolConfigRepository repository;
    @Mock
    private SymbolConfigCache cache;

    private SymbolConfigService service;

    private final SymbolConfig config = SymbolConfig.builder()
            .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
            .priceScale(2).quantityScale(6)
            .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
            .status(SymbolConfig.SymbolStatus.ACTIVE)
            .build();

    @BeforeEach
    void setUp() {
        service = new SymbolConfigService(repository, cache);
    }

    @Test
    void getRequired_cacheHit_returnsConfig() {
        when(cache.get("BTC_USDT")).thenReturn(config);
        assertThat(service.getRequired("BTC_USDT")).isSameAs(config);
        verify(repository, never()).findBySymbol(any());
    }

    @Test
    void getRequired_cacheMiss_loadsDbAndBackfills() {
        when(cache.get("BTC_USDT")).thenReturn(null);
        when(repository.findBySymbol("BTC_USDT")).thenReturn(config);

        SymbolConfig result = service.getRequired("BTC_USDT");

        assertThat(result).isSameAs(config);
        verify(cache).put("BTC_USDT", config);
    }

    @Test
    void getRequired_notFound_throws() {
        when(cache.get("BTC_USDT")).thenReturn(null);
        when(repository.findBySymbol("BTC_USDT")).thenReturn(null);

        assertThatThrownBy(() -> service.getRequired("BTC_USDT"))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("交易对不存在");
    }

    @Test
    void getRequired_paused_throws() {
        SymbolConfig paused = SymbolConfig.builder()
                .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
                .priceScale(2).quantityScale(6)
                .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
                .status(SymbolConfig.SymbolStatus.PAUSED)
                .build();
        when(cache.get("BTC_USDT")).thenReturn(paused);

        assertThatThrownBy(() -> service.getRequired("BTC_USDT"))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("暂停");
    }
}
