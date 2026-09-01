package com.cex.market.infrastructure.metrics;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** 定时使用 Kafka Admin API 采集 cex-market 消费组的真实分区 lag。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MarketKafkaLagMonitor {

    private final MarketMetrics marketMetrics;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${cex.market.metrics.kafka-lag-enabled:true}")
    private boolean enabled;

    @Value("${cex.market.metrics.kafka-lag-timeout-ms:5000}")
    private long timeoutMillis;

    /** 定时采集 Kafka 消费组已提交位点与分区末位点之差。 */
    @Scheduled(fixedDelayString = "${cex.market.metrics.kafka-lag-interval-ms:30000}")
    public void collect() {
        if (!enabled) {
            return;
        }
        try {
            validateConfiguration();
        } catch (IllegalStateException exception) {
            log.warn("Kafka lag 采集配置非法，不影响行情消费: {}", exception.getMessage());
            return;
        }
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = adminClient.listConsumerGroupOffsets("cex-market")
                    .partitionsToOffsetAndMetadata().get(timeoutMillis, TimeUnit.MILLISECONDS);
            if (committedOffsets.isEmpty()) {
                return;
            }
            Map<TopicPartition, OffsetSpec> endOffsetRequests = committedOffsets.keySet().stream()
                    .collect(Collectors.toMap(partition -> partition, partition -> OffsetSpec.latest()));
            Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> endOffsets = adminClient
                    .listOffsets(endOffsetRequests).all().get(timeoutMillis, TimeUnit.MILLISECONDS);
            committedOffsets.forEach((partition, committed) -> {
                var endOffset = endOffsets.get(partition);
                if (endOffset != null) {
                    marketMetrics.updateKafkaLag(partition.topic(), partition.partition(), endOffset.offset() - committed.offset());
                }
            });
        } catch (Exception exception) {
            log.warn("Kafka lag 采集失败，不影响行情消费: {}", exception.getMessage());
        }
    }

    /** 校验 Kafka lag 采集配置。 */
    private void validateConfiguration() {
        if (bootstrapServers == null || bootstrapServers.isBlank() || timeoutMillis <= 0) {
            throw new IllegalStateException("Kafka lag 监控配置非法");
        }
    }
}
