package com.cex.clearing.infrastructure.scheduler;

import com.cex.clearing.application.service.SettlementRecoveryApplicationService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 定时接管超时结算任务，并调度数据库驱动的延迟重试。 */
@Component
@RequiredArgsConstructor
public class SettlementRecoveryJob {

    private final SettlementRecoveryApplicationService recoveryApplicationService;

    /** 每秒扫描一次；并发实例通过任务状态 CAS 避免重复入账。 */
    @Scheduled(fixedDelay = 1000, initialDelay = 10000)
    public void recoverAndRetry() {
        recoveryApplicationService.recoverAndRetry();
    }
}
