package com.cex.market.infrastructure.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** 行情服务核心链路的 Micrometer 指标出口。 */
@Component
@RequiredArgsConstructor
public class MarketMetrics {

    private final MeterRegistry meterRegistry;
    private final AtomicInteger webSocketConnections = new AtomicInteger();
    private final ConcurrentHashMap<KafkaPartition, AtomicLong> kafkaLags = new ConcurrentHashMap<>();

    /** 初始化长期存在的 Gauge 指标。 */
    public void initialize() {
        Gauge.builder("cex.market.websocket.connections", webSocketConnections, AtomicInteger::get)
                .description("当前市场 WebSocket 连接数").register(meterRegistry);
    }

    /**
     * 更新当前 WebSocket 连接数。
     *
     * @param connectionCount 活跃连接数
     */
    public void updateWebSocketConnections(int connectionCount) {
        webSocketConnections.set(Math.max(connectionCount, 0));
    }

    /** 记录一次慢客户端保护性断开。 */
    public void recordSlowConsumerDisconnect() {
        Counter.builder("cex.market.websocket.slow_consumer_disconnect").description("慢客户端保护性断开次数")
                .register(meterRegistry).increment();
    }

    /** 记录一次订单簿 sequence gap。 */
    public void recordSequenceGap() {
        Counter.builder("cex.market.orderbook.sequence_gap").description("订单簿序号缺口次数")
                .register(meterRegistry).increment();
    }

    /**
     * 记录一次订单簿恢复结果。
     *
     * @param success 是否恢复成功
     */
    public void recordOrderBookRecovery(boolean success) {
        Counter.builder("cex.market.orderbook.recovery").tag("result", success ? "success" : "failure")
                .description("订单簿恢复次数").register(meterRegistry).increment();
    }

    /**
     * 记录事件从产生到进入行情消费端的延迟。
     *
     * @param eventType 事件类型
     * @param eventTime 事件产生时间（UTC 毫秒）
     */
    public void recordEventDelay(String eventType, long eventTime) {
        long delayMillis = Math.max(0L, System.currentTimeMillis() - eventTime);
        Timer.builder("cex.market.event.delay").tag("event_type", eventType).publishPercentileHistogram()
                .description("行情事件消费延迟").register(meterRegistry).record(Duration.ofMillis(delayMillis));
    }

    /**
     * 更新一个 Kafka Topic 分区的消费者滞后。
     *
     * @param topic Kafka Topic
     * @param partition Kafka 分区
     * @param lag 当前 lag
     */
    public void updateKafkaLag(String topic, int partition, long lag) {
        KafkaPartition key = new KafkaPartition(topic, partition);
        AtomicLong value = kafkaLags.computeIfAbsent(key, ignored -> {
            AtomicLong gaugeValue = new AtomicLong();
            Gauge.builder("cex.market.kafka.lag", gaugeValue, AtomicLong::get).tag("topic", topic)
                    .tag("partition", String.valueOf(partition)).description("行情消费者 Kafka lag")
                    .register(meterRegistry);
            return gaugeValue;
        });
        value.set(Math.max(lag, 0L));
    }

    /** Kafka Topic 分区标识。 */
    private record KafkaPartition(String topic, int partition) {
    }
}
