package com.cex.matching.infrastructure.kafka;

import com.cex.matching.application.service.MatchingSnapshotService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Collection;

/** 分区释放前保存命令边界快照，缩短接管实例的恢复时间。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MatchingConsumerRebalanceListener implements ConsumerRebalanceListener {
    private final MatchingSnapshotService matchingSnapshotService;

    /**
     * 在容器停止拉取新记录后写入全部当前订单簿快照。
     *
     * @param partitions 即将释放的分区
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("撮合分区即将释放，开始保存快照: partitions={}", partitions);
        matchingSnapshotService.saveAllSnapshots();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("撮合分区已分配: partitions={}", partitions);
    }
}
