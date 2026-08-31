package com.cex.clearing.infrastructure.kafka;

import com.cex.clearing.application.service.SettlementOutboxRelayService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 定时扫描结算发件箱。 */
@Component
@RequiredArgsConstructor
public class SettlementOutboxRelay {
    private final SettlementOutboxRelayService relayService;

    @Scheduled(fixedDelay = 1000, initialDelay = 5000)
    public void relay() {
        relayService.relay();
    }
}
