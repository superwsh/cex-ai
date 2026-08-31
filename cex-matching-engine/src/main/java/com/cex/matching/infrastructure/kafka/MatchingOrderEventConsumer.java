package com.cex.matching.infrastructure.kafka;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.OrderEvent;
import com.cex.matching.core.MatchingEngine;
import com.cex.matching.application.service.PeriodicMatchingSnapshotService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import com.cex.matching.domain.sequence.SequenceGapException;
import org.springframework.stereotype.Component;

/** 消费订单事件并将撮合成交投递到成交主题。 */
@Component
@RequiredArgsConstructor
public class MatchingOrderEventConsumer {

    private final MatchingEngine matchingEngine;
    private final MatchingTradeEventProducer matchingTradeEventProducer;
    private final MatchingOrderResultEventProducer matchingOrderResultEventProducer;
    private final PeriodicMatchingSnapshotService periodicMatchingSnapshotService;

    /**
     * 在订单事件消费成功后同步发布全部成交事件。
     * 订单消息以 symbol 为键分区，同一交易对由同一分区的单个消费者线程顺序处理。
     *
     * @param event 订单服务经本地消息表投递的订单事件
     */
    @KafkaListener(topics = TopicConstants.TOPIC_ORDER_EVENT, groupId = "cex-matching", autoStartup = "false",
            containerFactory = "matchingKafkaListenerContainerFactory")
    public void onOrderEvent(OrderEvent event, Acknowledgment acknowledgment,
                             Consumer<?, ?> consumer,
                             @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                             @Header(KafkaHeaders.OFFSET) long offset) {
        try {
            matchingEngine.match(event, matchingTradeEventProducer::send, matchingOrderResultEventProducer::send);
            periodicMatchingSnapshotService.snapshotIfDue(event);
            acknowledgment.acknowledge();
        } catch (SequenceGapException exception) {
            consumer.pause(java.util.Set.of(new TopicPartition(TopicConstants.TOPIC_ORDER_EVENT, partition)));
            throw new IllegalStateException("撮合分区因 sequence gap 已暂停: symbol=" + event.getSymbol()
                    + ", sequence=" + event.getSequence() + ", partition=" + partition + ", offset=" + offset,
                    exception);
        }
    }
}
