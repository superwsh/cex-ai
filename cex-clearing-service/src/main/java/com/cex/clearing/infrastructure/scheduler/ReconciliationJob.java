package com.cex.clearing.infrastructure.scheduler;

import com.cex.clearing.application.service.ReconciliationApplicationService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 定时执行只读对账，问题结果交由后续运营后台处置。 */
@Component
@RequiredArgsConstructor
public class ReconciliationJob {

    private final ReconciliationApplicationService reconciliationApplicationService;

    /** 每分钟执行一次，扫描过程不修改账户、流水或历史凭证。 */
    @Scheduled(fixedDelay = 60000, initialDelay = 30000)
    public void reconcile() {
        reconciliationApplicationService.reconcile();
    }
}
