package com.cex.matching.application.service;

import com.cex.common.kafka.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** 在同一交易对命令边界按间隔保存订单簿快照，避免独立线程并发读取订单簿。 */
@Component
@RequiredArgsConstructor
public class PeriodicMatchingSnapshotService {

    private final MatchingSnapshotService matchingSnapshotService;
    private final ConcurrentHashMap<String, AtomicLong> processedCommandCounts = new ConcurrentHashMap<>();

    @Value("${cex.matching.snapshot-interval:1000}")
    private long snapshotInterval;

    @Value("${cex.matching.snapshot-enabled:true}")
    private boolean snapshotEnabled;

    /**
     * 在一条订单命令及其成交事件成功处理后，按配置间隔保存快照。
     *
     * @param event 已成功处理的订单事件
     */
    public void snapshotIfDue(OrderEvent event) {
        long commandCount = processedCommandCounts.computeIfAbsent(event.getSymbol(), key -> new AtomicLong())
                .incrementAndGet();
        if (snapshotEnabled && snapshotInterval > 0 && commandCount % snapshotInterval == 0) {
            matchingSnapshotService.saveSnapshot(event.getSymbol());
        }
    }
}
