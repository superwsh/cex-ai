package com.cex.matching.application.service;

import com.cex.matching.domain.model.OrderBookSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** 按“快照在前、WAL 在后”的顺序恢复单个交易对热状态。 */
@Component
public class MatchingRecoveryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MatchingRecoveryService.class);

    private final MatchingEngineRegistry matchingEngineRegistry;
    private final MatchingSnapshotRepository snapshotRepository;
    private final MatchingCommandJournal commandJournal;
    private final MatchingMetrics matchingMetrics;

    /**
     * 创建不写入运行时监控的恢复服务，供单元测试使用。
     *
     * @param matchingEngineRegistry 撮合实例注册表
     * @param snapshotRepository 快照仓库
     * @param commandJournal 命令预写日志
     */
    public MatchingRecoveryService(MatchingEngineRegistry matchingEngineRegistry,
                                   MatchingSnapshotRepository snapshotRepository,
                                   MatchingCommandJournal commandJournal) {
        this(matchingEngineRegistry, snapshotRepository, commandJournal, MatchingMetrics.noop());
    }

    /**
     * 创建启动恢复服务。
     *
     * @param matchingEngineRegistry 撮合实例注册表
     * @param snapshotRepository 快照仓库
     * @param commandJournal 命令预写日志
     * @param matchingMetrics 重放指标记录器
     */
    @Autowired
    public MatchingRecoveryService(MatchingEngineRegistry matchingEngineRegistry,
                                   MatchingSnapshotRepository snapshotRepository,
                                   MatchingCommandJournal commandJournal,
                                   MatchingMetrics matchingMetrics) {
        this.matchingEngineRegistry = matchingEngineRegistry;
        this.snapshotRepository = snapshotRepository;
        this.commandJournal = commandJournal;
        this.matchingMetrics = matchingMetrics;
    }

    /**
     * 恢复指定交易对，WAL 重放中发现序号不连续时立即失败。
     *
     * @param symbol 需要恢复的交易对
     */
    public void recover(String symbol) {
        long startedAt = System.nanoTime();
        OrderBookSnapshot snapshot = snapshotRepository.load(symbol).orElse(null);
        long snapshotSequence = snapshot == null ? 0L : snapshot.sequence();
        if (snapshot != null) {
            matchingEngineRegistry.restore(snapshot);
        }
        matchingEngineRegistry.replay(symbol, snapshotSequence);
        matchingMetrics.recordReplay(symbol, java.time.Duration.ofNanos(System.nanoTime() - startedAt));
        LOGGER.info("撮合状态恢复完成: symbol={}, snapshotSequence={}", symbol, snapshotSequence);
    }

    /** 启动时恢复 WAL 中全部交易对。 */
    public void recoverAll() {
        for (String symbol : commandJournal.symbols()) {
            recover(symbol);
        }
    }
}
