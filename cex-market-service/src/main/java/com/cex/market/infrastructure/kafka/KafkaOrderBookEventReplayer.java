package com.cex.market.infrastructure.kafka;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.market.OrderBookDeltaEvent;
import com.cex.market.application.service.OrderBookEventReplayer;
import com.cex.market.domain.model.MarketOrderBookSnapshot;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** 使用独立 Kafka Consumer 从持久化快照位点重放订单簿 WAL。 */
@Component
@RequiredArgsConstructor
public class KafkaOrderBookEventReplayer implements OrderBookEventReplayer {

    private final ConsumerFactory<String, OrderBookDeltaEvent> consumerFactory;

    @Value("${cex.market.recovery.replay-timeout-ms:10000}")
    private long replayTimeoutMillis;

    /**
     * 从快照后一个 Kafka 位点开始重放到目标位点。
     *
     * @param snapshot 持久化恢复快照
     * @param targetPartition 目标分区
     * @param targetOffset 目标位点
     * @return 目标交易对的有序盘口增量
     */
    @Override
    public List<OrderBookDeltaEvent> replayTo(MarketOrderBookSnapshot snapshot, int targetPartition, long targetOffset) {
        if (snapshot.kafkaPartition() != targetPartition || targetOffset < snapshot.kafkaOffset()) {
            throw new IllegalArgumentException("Kafka 重放目标与订单簿快照不匹配");
        }
        if (targetOffset == snapshot.kafkaOffset()) {
            return List.of();
        }
        TopicPartition partition = new TopicPartition(TopicConstants.TOPIC_ORDER_BOOK_DELTA_EVENT, targetPartition);
        try (Consumer<String, OrderBookDeltaEvent> consumer = consumerFactory.createConsumer(
                "cex-market-recovery-" + UUID.randomUUID())) {
            consumer.assign(List.of(partition));
            consumer.seek(partition, snapshot.kafkaOffset() + 1);
            long deadline = System.currentTimeMillis() + replayTimeoutMillis;
            List<OrderBookDeltaEvent> events = new ArrayList<>();
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, OrderBookDeltaEvent> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.offset() > targetOffset) {
                        throw new IllegalStateException("Kafka 重放越过目标位点");
                    }
                    OrderBookDeltaEvent event = record.value();
                    if (event != null && snapshot.symbol().equals(event.getSymbol())
                            && event.getSequence() > snapshot.sequence()) {
                        events.add(event);
                    }
                    if (record.offset() == targetOffset) {
                        return List.copyOf(events);
                    }
                }
            }
        }
        throw new IllegalStateException("Kafka 重放超时: symbol=" + snapshot.symbol() + ", targetOffset=" + targetOffset);
    }
}
