package com.cex.clearing.application.observability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** 清算指标注册和更新测试。 */
class ClearingMetricsTest {

    /** 应注册验收要求中的核心结算、对账和 Outbox 指标。 */
    @Test
    void shouldRegisterAndUpdateCoreMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ClearingMetrics metrics = new ClearingMetrics(registry);

        metrics.recordSettlementSuccess(System.nanoTime());
        metrics.recordSettlementFailure("INSUFFICIENT_SETTLEMENT_BALANCE");
        metrics.recordSettlementFailure("DATABASE_DEADLOCK");
        metrics.recordSettlementRetry();
        metrics.recordManualReview();
        metrics.recordDuplicateTrade();
        metrics.recordReconciliationDifferences(2);
        metrics.setOutboxPending(3);
        metrics.setOutboxFailed(1);
        metrics.recordConsumerLag(System.currentTimeMillis() - 10);

        assertThat(registry.get("settlement.success").counter().count()).isEqualTo(1);
        assertThat(registry.get("settlement.failed").counter().count()).isEqualTo(2);
        assertThat(registry.get("settlement.insufficient.balance").counter().count()).isEqualTo(1);
        assertThat(registry.get("reconciliation.difference").counter().count()).isEqualTo(2);
        assertThat(registry.get("outbox.pending.total").gauge().value()).isEqualTo(3);
        assertThat(registry.get("outbox.failed.total").gauge().value()).isEqualTo(1);
        assertThat(registry.get("settlement.db.deadlock").counter().count()).isEqualTo(1);
        assertThat(registry.get("settlement.consumer.lag").gauge().value()).isGreaterThanOrEqualTo(0);
    }
}
