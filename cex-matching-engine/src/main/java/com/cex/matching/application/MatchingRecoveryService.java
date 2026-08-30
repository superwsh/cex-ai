package com.cex.matching.application;

import com.cex.matching.domain.engine.InMemoryCommandMatchingEngine;
import com.cex.matching.domain.model.MatchingCommand;
import com.cex.matching.domain.sequence.SequenceGapException;
import com.cex.matching.domain.sequence.SequenceManager;
import com.cex.matching.infrastructure.snapshot.SnapshotManager;
import com.cex.matching.infrastructure.wal.WalReader;
import com.cex.matching.infrastructure.wal.WalRecord;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** 通过最新有效快照与后续 WAL 重建单交易对内存状态。 */
public final class MatchingRecoveryService {
    private final SnapshotManager snapshotManager;
    private final WalReader walReader;
    private final InMemoryCommandMatchingEngine engine;
    private final MatchingCommandHandler handler;
    private final SequenceManager sequenceManager;
    private final ConcurrentHashMap<String, Object> recoveryLocks = new ConcurrentHashMap<>();

    /**
     * 创建撮合恢复服务。
     *
     * @param snapshotManager 快照管理器
     * @param walReader WAL 读取器
     * @param engine 内存撮合引擎
     * @param handler 命令处理器
     * @param sequenceManager 序列管理器
     */
    public MatchingRecoveryService(SnapshotManager snapshotManager, WalReader walReader, InMemoryCommandMatchingEngine engine, MatchingCommandHandler handler, SequenceManager sequenceManager) {
        this.snapshotManager = Objects.requireNonNull(snapshotManager, "快照管理器不能为空");
        this.walReader = Objects.requireNonNull(walReader, "WAL 读取器不能为空");
        this.engine = Objects.requireNonNull(engine, "内存撮合引擎不能为空");
        this.handler = Objects.requireNonNull(handler, "命令处理器不能为空");
        this.sequenceManager = Objects.requireNonNull(sequenceManager, "序列管理器不能为空");
    }

    /**
     * 恢复交易对订单簿：先装载快照，再按严格序号重放后续 WAL。
     *
     * 仅当订单簿重放及序列水位恢复均成功后，才解除在线命令封锁。
     *
     * @param symbol 待恢复交易对
     */
    public void recover(String symbol) {
        String matchingSymbol = Objects.requireNonNull(symbol, "交易对不能为空");
        synchronized (lockOf(matchingSymbol)) {
            recoverSymbol(matchingSymbol);
        }
    }

    /**
     * 在同交易对恢复会话锁内执行完整恢复过程。
     *
     * @param symbol 待恢复交易对
     */
    private void recoverSymbol(String symbol) {
        handler.markRecoveryStarted(symbol);
        engine.reset(symbol);
        long restoredSequence = restoreSnapshot(symbol);
        for (WalRecord record : sortedRecords(symbol)) {
            if (record.sequence() <= restoredSequence) {
                continue;
            }
            ensureNextSequence(symbol, restoredSequence, record.sequence());
            handler.replay(commandOf(record));
            restoredSequence = record.sequence();
        }
        sequenceManager.restore(symbol, restoredSequence);
        handler.markRecoveryCompleted(symbol);
    }

    /**
     * 从最新有效快照恢复订单簿基线。
     *
     * @param symbol 交易对
     * @return 快照对应的最后序列；没有快照时为零
     */
    private long restoreSnapshot(String symbol) {
        return snapshotManager.loadLatest(symbol)
                .map(snapshot -> {
                    ensureSnapshotSymbol(symbol, snapshot.symbol());
                    engine.restore(snapshot);
                    return snapshot.lastSequence();
                })
                .orElse(0L);
    }

    /**
     * 校验快照内容属于当前恢复交易对。
     *
     * @param symbol 当前恢复交易对
     * @param snapshotSymbol 快照内容中的交易对
     */
    private void ensureSnapshotSymbol(String symbol, String snapshotSymbol) {
        if (!symbol.equals(snapshotSymbol)) {
            throw new IllegalArgumentException("快照交易对与恢复目标不一致");
        }
    }

    /**
     * 读取、校验并按序列升序排列 WAL 记录。
     *
     * @param symbol 交易对
     * @return 已排序的 WAL 记录
     */
    private List<WalRecord> sortedRecords(String symbol) {
        return walReader.read(symbol).records().stream()
                .peek(record -> ensureRecordSymbol(symbol, record))
                .sorted(Comparator.comparingLong(WalRecord::sequence))
                .toList();
    }

    /**
     * 校验 WAL 记录属于当前恢复交易对。
     *
     * @param symbol 当前恢复交易对
     * @param record WAL 记录
     */
    private void ensureRecordSymbol(String symbol, WalRecord record) {
        if (!symbol.equals(record.symbol())) {
            throw new IllegalArgumentException("WAL 记录交易对与恢复目标不一致");
        }
    }

    /**
     * 校验待重放 WAL 的序列紧邻已恢复水位。
     *
     * @param symbol 交易对
     * @param restoredSequence 当前已恢复水位
     * @param actualSequence 待重放序列
     */
    private void ensureNextSequence(String symbol, long restoredSequence, long actualSequence) {
        if (actualSequence != restoredSequence + 1L) {
            throw new SequenceGapException(symbol, restoredSequence + 1L, actualSequence);
        }
    }

    /**
     * 将 WAL 记录转换为重放命令。
     *
     * @param record WAL 记录
     * @return 对应撮合命令
     */
    private MatchingCommand commandOf(WalRecord record) {
        return new MatchingCommand(record.sequence(), record.commandId(), record.orderId(), record.userId(),
                record.symbol(), record.commandType(), record.side(), record.price(), record.quantity(),
                record.timestamp());
    }

    /**
     * 获取单个交易对的恢复会话锁。
     *
     * 该锁覆盖从封锁在线处理到恢复完成解锁的整个过程，避免相同交易对的两个恢复会话交错。
     *
     * @param symbol 交易对
     * @return 交易对对应的恢复会话锁
     */
    private Object lockOf(String symbol) {
        return recoveryLocks.computeIfAbsent(symbol, ignored -> new Object());
    }
}
