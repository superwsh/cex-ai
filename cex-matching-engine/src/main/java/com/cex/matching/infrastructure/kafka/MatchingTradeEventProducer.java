package com.cex.matching.infrastructure.kafka;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.TradeEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** 将公共成交事件可靠投递给下游服务。 */
@Component
@RequiredArgsConstructor
public class MatchingTradeEventProducer {

    private static final long SEND_TIMEOUT_SECONDS = 5L;

    private final KafkaTemplate<String, TradeEvent> kafkaTemplate;

    /**
     * 以交易对作为 Kafka 键同步发送成交事件。
     * 发送失败时抛出异常，使订单事件不被确认并由 Kafka 重试，避免撮合结果静默丢失。
     *
     * @param event 应投递至成交主题的公共成交事件
     */
    public void send(TradeEvent event) {
        try {
            kafkaTemplate.send(TopicConstants.TOPIC_TRADE_EVENT, event.getSymbol(), event)
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("成交事件发送被中断: tradeId=" + event.getTradeId(), exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("成交事件发送失败: tradeId=" + event.getTradeId(), exception);
        }
    }
}
