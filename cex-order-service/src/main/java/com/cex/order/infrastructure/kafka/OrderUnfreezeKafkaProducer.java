package com.cex.order.infrastructure.kafka;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.OrderUnfreezeEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/** 将订单剩余冻结释放指令投递给清算服务。 */
@Component
@RequiredArgsConstructor
public class OrderUnfreezeKafkaProducer {

    private final KafkaTemplate<String, OrderUnfreezeEvent> kafkaTemplate;

    /** 以 orderId 作为消息键投递解冻事件。 */
    public void send(OrderUnfreezeEvent event) {
        try {
            kafkaTemplate.send(TopicConstants.TOPIC_ORDER_UNFREEZE_EVENT, String.valueOf(event.getOrderId()), event)
                    .get(5, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("订单剩余冻结解冻事件发送被中断: orderId=" + event.getOrderId(), exception);
        } catch (Exception exception) {
            throw new IllegalStateException("订单剩余冻结解冻事件发送失败: orderId=" + event.getOrderId(), exception);
        }
    }
}
