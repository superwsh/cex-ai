package com.cex.market.infrastructure.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** 行情 Micrometer 指标测试。 */
class MarketMetricsTest {

    @Test
    void shouldRecordCoreMarketMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MarketMetrics metrics = new MarketMetrics(registry);
        metrics.initialize();

        metrics.updateWebSocketConnections(3);
        metrics.recordSlowConsumerDisconnect();
        metrics.recordSequenceGap();
        metrics.recordOrderBookRecovery(true);
        metrics.recordEventDelay("trade", System.currentTimeMillis() - 10);
        metrics.updateKafkaLag("cex.trade.event", 1, 12L);

        assertThat(registry.find("cex.market.websocket.connections").gauge().value()).isEqualTo(3D);
        assertThat(registry.find("cex.market.websocket.slow_consumer_disconnect").counter().count()).isEqualTo(1D);
        assertThat(registry.find("cex.market.orderbook.sequence_gap").counter().count()).isEqualTo(1D);
        assertThat(registry.find("cex.market.orderbook.recovery").tag("result", "success").counter().count()).isEqualTo(1D);
        assertThat(registry.find("cex.market.event.delay").tag("event_type", "trade").timer().count()).isEqualTo(1L);
        assertThat(registry.find("cex.market.kafka.lag").tag("topic", "cex.trade.event").tag("partition", "1")
                .gauge().value()).isEqualTo(12D);
    }
}
