package com.cex.order.infrastructure.kafka;

import com.cex.order.application.service.OutboxRelayService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Outbox Relay 定时调度:每 1 秒扫描一次
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxRelayService relayService;

    @Scheduled(fixedDelay = 1000, initialDelay = 5000)
    public void scan() {
        try {
            relayService.relay();
        } catch (Exception e) {
            log.error("Outbox Relay 执行异常", e);
        }
    }
}
