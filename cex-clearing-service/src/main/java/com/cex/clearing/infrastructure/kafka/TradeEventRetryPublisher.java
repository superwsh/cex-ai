package com.cex.clearing.infrastructure.kafka;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.TradeEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.kafka.support.KafkaHeaders;

/** 成交事件 Retry / DLQ 发布器。 */
@Component
@RequiredArgsConstructor
public class TradeEventRetryPublisher {

    public static final String RETRY_COUNT_HEADER = "x-cex-retry-count";
    public static final String ERROR_CODE_HEADER = "x-cex-settlement-error-code";

    private final KafkaTemplate<String, TradeEvent> kafkaTemplate;

    /** 发送到重试主题，并在 Header 中携带本次重试次数。 */
    public void publishRetry(TradeEvent event, int retryCount, String errorCode) {
        kafkaTemplate.send(MessageBuilder.withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, TopicConstants.TOPIC_TRADE_EVENT_RETRY)
                .setHeader(KafkaHeaders.KEY, event.getSymbol())
                .setHeader(RETRY_COUNT_HEADER, retryCount)
                .setHeader(ERROR_CODE_HEADER, errorCode)
                .build());
    }

    /** 发送到死信主题，供人工复核和告警系统处理。 */
    public void publishDlq(TradeEvent event, String errorCode) {
        kafkaTemplate.send(MessageBuilder.withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, TopicConstants.TOPIC_TRADE_EVENT_DLQ)
                .setHeader(KafkaHeaders.KEY, event == null ? null : event.getSymbol())
                .setHeader(ERROR_CODE_HEADER, errorCode)
                .build());
    }
}
