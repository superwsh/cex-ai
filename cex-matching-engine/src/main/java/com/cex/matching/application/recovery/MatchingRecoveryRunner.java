package com.cex.matching.application.recovery;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

/** 在启动订单事件消费者前完成 WAL 与快照恢复。 */
@Component
@RequiredArgsConstructor
public class MatchingRecoveryRunner implements ApplicationRunner {

    private final MatchingRecoveryService matchingRecoveryService;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    /**
     * 恢复全部持久化交易对状态。
     *
     * @param args 应用启动参数
     */
    @Override
    public void run(ApplicationArguments args) {
        matchingRecoveryService.recoverAll();
        kafkaListenerEndpointRegistry.start();
    }
}
