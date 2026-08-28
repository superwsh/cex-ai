package com.cex.order.infrastructure.asset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MockAccountServiceClientTest {

    private MockAccountServiceClient client;

    @BeforeEach
    void setUp() {
        // 预置:用户 100 有 10000 USDT 与 1 BTC 可用余额
        Map<String, BigDecimal> balances = new HashMap<>();
        balances.put("100:USDT", new BigDecimal("10000"));
        balances.put("100:BTC", new BigDecimal("1"));
        client = new MockAccountServiceClient(balances);
    }

    @Test
    void freeze_deductsAvailableAndFreezes() {
        client.freeze(FreezeRequest.builder()
                .userId(100L).currency("USDT").amount(new BigDecimal("5000"))
                .bizType("FREEZE_ORDER").bizId(1L).build());

        assertThat(client.getAvailable(100L, "USDT")).isEqualByComparingTo("5000");
        assertThat(client.getFrozen(100L, "USDT")).isEqualByComparingTo("5000");
    }

    @Test
    void freeze_sameBizId_idempotent() {
        FreezeRequest request = FreezeRequest.builder()
                .userId(100L).currency("USDT").amount(new BigDecimal("5000"))
                .bizType("FREEZE_ORDER").bizId(1L).build();
        client.freeze(request);
        assertThatCode(() -> client.freeze(request)).doesNotThrowAnyException();
        // 重复冻结不重复扣减
        assertThat(client.getFrozen(100L, "USDT")).isEqualByComparingTo("5000");
        assertThat(client.getAvailable(100L, "USDT")).isEqualByComparingTo("5000");
    }

    @Test
    void freeze_insufficientBalance_throws() {
        assertThatThrownBy(() -> client.freeze(FreezeRequest.builder()
                        .userId(100L).currency("USDT").amount(new BigDecimal("99999"))
                        .bizType("FREEZE_ORDER").bizId(2L).build()))
                .hasMessageContaining("余额不足");
    }

    @Test
    void unfreeze_releasesFrozen() {
        client.freeze(FreezeRequest.builder()
                .userId(100L).currency("USDT").amount(new BigDecimal("5000"))
                .bizType("FREEZE_ORDER").bizId(1L).build());
        client.unfreeze(UnfreezeRequest.builder()
                .userId(100L).currency("USDT").amount(new BigDecimal("2000"))
                .bizType("FREEZE_ORDER").bizId(1L).build());

        assertThat(client.getFrozen(100L, "USDT")).isEqualByComparingTo("3000");
        assertThat(client.getAvailable(100L, "USDT")).isEqualByComparingTo("7000");
    }

    @Test
    void unfreeze_moreThanFrozen_throws() {
        client.freeze(FreezeRequest.builder()
                .userId(100L).currency("USDT").amount(new BigDecimal("5000"))
                .bizType("FREEZE_ORDER").bizId(1L).build());
        assertThatThrownBy(() -> client.unfreeze(UnfreezeRequest.builder()
                        .userId(100L).currency("USDT").amount(new BigDecimal("9999"))
                        .bizType("FREEZE_ORDER").bizId(1L).build()))
                .hasMessageContaining("解冻金额超过冻结金额");
    }
}
