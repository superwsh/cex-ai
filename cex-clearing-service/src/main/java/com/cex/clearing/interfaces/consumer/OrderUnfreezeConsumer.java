package com.cex.clearing.interfaces.consumer;

import com.cex.clearing.application.service.AccountCommandApplicationService;
import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.OrderUnfreezeEvent;
import com.cex.common.dubbo.account.UnfreezeBalanceRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** 消费订单终态后的解冻指令，落入本地账户事务及幂等流水。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderUnfreezeConsumer {

    private final AccountCommandApplicationService accountCommandApplicationService;

    /** 执行订单剩余冻结解冻；账户命令表负责重复消息幂等。 */
    @KafkaListener(topics = TopicConstants.TOPIC_ORDER_UNFREEZE_EVENT, groupId = "cex-clearing")
    public void onOrderUnfreeze(OrderUnfreezeEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("订单解冻事件不能为空");
        }
        accountCommandApplicationService.unfreeze(new UnfreezeBalanceRequest(event.getUserId(), event.getAsset(),
                event.getAmount(), "FREEZE_ORDER", String.valueOf(event.getOrderId())));
        log.info("订单剩余冻结已释放: eventId={}, orderId={}, userId={}, asset={}, amount={}",
                event.getEventId(), event.getOrderId(), event.getUserId(), event.getAsset(), event.getAmount());
    }
}
