package com.cex.matching.application.service;

import com.cex.matching.domain.model.OrderBookSnapshot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** 在命令处理边界将指定交易对的当前热状态保存为快照。 */
@Component
public class MatchingSnapshotService {

    private final MatchingEngineRegistry matchingEngineRegistry;
    private final MatchingSnapshotRepository snapshotRepository;
    private final MatchingCommandJournal commandJournal;
    private final MatchingMetrics matchingMetrics;

    /**
     * 创建不写入运行时监控的快照服务，供单元测试使用。
     *
     * @param matchingEngineRegistry 撮合实例注册表
     * @param snapshotRepository 快照仓库
     * @param commandJournal 命令预写日志
     */
    public MatchingSnapshotService(MatchingEngineRegistry matchingEngineRegistry,
                                   MatchingSnapshotRepository snapshotRepository,
                                   MatchingCommandJournal commandJournal) {
        this(matchingEngineRegistry, snapshotRepository, commandJournal, MatchingMetrics.noop());
    }

    /**
     * 创建持久化快照服务。
     *
     * @param matchingEngineRegistry 撮合实例注册表
     * @param snapshotRepository 快照仓库
     * @param commandJournal 命令预写日志
     * @param matchingMetrics 快照指标记录器
     */
    @Autowired
    public MatchingSnapshotService(MatchingEngineRegistry matchingEngineRegistry,
                                   MatchingSnapshotRepository snapshotRepository,
                                   MatchingCommandJournal commandJournal,
                                   MatchingMetrics matchingMetrics) {
        this.matchingEngineRegistry = matchingEngineRegistry;
        this.snapshotRepository = snapshotRepository;
        this.commandJournal = commandJournal;
        this.matchingMetrics = matchingMetrics;
    }

    /**
     * 保存指定交易对的当前订单簿快照。
     *
     * @param symbol 需要保存快照的交易对
     * @return 已保存的快照；交易对尚未初始化时返回空值
     */
    public java.util.Optional<OrderBookSnapshot> saveSnapshot(String symbol) {
        long startedAt = System.nanoTime();
        java.util.Optional<OrderBookSnapshot> snapshot = matchingEngineRegistry.snapshot(symbol);
        snapshot.ifPresent(value -> saveAndCompact(value, startedAt));
        return snapshot;
    }

    /** 在 Kafka 分区释放前保存当前全部订单簿。 */
    public void saveAllSnapshots() {
        matchingEngineRegistry.activeSymbols().forEach(this::saveSnapshot);
    }

    /**
     * 先持久化完整快照，成功后才裁剪已被快照覆盖的 WAL。
     *
     * @param snapshot 即将持久化的订单簿快照
     * @param startedAt 创建快照操作开始时的单调时间戳
     */
    private void saveAndCompact(OrderBookSnapshot snapshot, long startedAt) {
        snapshotRepository.save(snapshot);
        matchingEngineRegistry.confirmSnapshotPersisted(snapshot.symbol());
        commandJournal.compact(snapshot.symbol(), snapshot.sequence());
        matchingMetrics.recordSnapshot(snapshot.symbol(), java.time.Duration.ofNanos(System.nanoTime() - startedAt));
    }
}
