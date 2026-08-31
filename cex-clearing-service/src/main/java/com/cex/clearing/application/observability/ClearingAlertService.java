package com.cex.clearing.application.observability;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** 输出不含敏感信息的结构化资金告警，供日志告警和 Prometheus 规则消费。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClearingAlertService {

    private final ClearingMetrics clearingMetrics;

    /** 记录需要运营或值班人员介入的清算异常。 */
    public void alert(String alertType, String bizId, String errorCode) {
        clearingMetrics.recordAlert();
        log.error("清算资金告警: alertType={}, bizId={}, errorCode={}", alertType, bizId, errorCode);
    }
}
