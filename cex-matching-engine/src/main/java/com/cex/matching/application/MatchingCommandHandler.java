package com.cex.matching.application;

import com.cex.matching.domain.engine.CommandMatchingEngine;
import com.cex.matching.domain.engine.ExecutionMode;
import com.cex.matching.domain.model.MatchingCommand;
import com.cex.matching.domain.sequence.SequenceManager;
import com.cex.matching.domain.sequence.SequenceValidation;
import com.cex.matching.infrastructure.wal.WalAppender;
import com.cex.matching.infrastructure.wal.WalRecord;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 编排可靠撮合命令处理的应用服务。
 *
 * LIVE 模式严格执行序列校验、WAL 同步落盘、领域状态变更和序列推进；REPLAY 模式仅重放领域状态。
 */
public final class MatchingCommandHandler {

    private final SequenceManager sequenceManager;
    private final WalAppender walAppender;
    private final CommandMatchingEngine commandMatchingEngine;
    private final ConcurrentHashMap<String, Object> symbolLocks = new ConcurrentHashMap<>();
    private final Set<String> recoveryRequiredSymbols = ConcurrentHashMap.newKeySet();

    /**
     * 创建命令处理器。
     *
     * @param sequenceManager 按交易对校验和推进序列的管理器
     * @param walAppender 同步追加 WAL 的基础设施端口
     * @param commandMatchingEngine 执行内存领域状态变更的撮合引擎
     */
    public MatchingCommandHandler(SequenceManager sequenceManager, WalAppender walAppender,
                                  CommandMatchingEngine commandMatchingEngine) {
        this.sequenceManager = Objects.requireNonNull(sequenceManager, "序列管理器不能为空");
        this.walAppender = Objects.requireNonNull(walAppender, "WAL 追加器不能为空");
        this.commandMatchingEngine = Objects.requireNonNull(commandMatchingEngine, "命令撮合引擎不能为空");
    }

    /**
     * 以在线语义处理命令，确保序列只在 WAL 和领域执行均成功后推进。
     *
     * @param command 待处理的撮合命令
     * @return 命令已应用或因重复而忽略的处理结果
     */
    public CommandHandlingResult handle(MatchingCommand command) {
        MatchingCommand matchingCommand = Objects.requireNonNull(command, "撮合命令不能为空");
        synchronized (lockOf(matchingCommand.symbol())) {
            ensureRecoveryNotRequired(matchingCommand.symbol());
            return handleLiveCommand(matchingCommand);
        }
    }

    /**
     * 在同一交易对的串行临界区内按在线语义处理命令。
     *
     * @param command 已完成空值校验的撮合命令
     * @return 命令已应用或因重复而忽略的处理结果
     */
    private CommandHandlingResult handleLiveCommand(MatchingCommand command) {
        if (sequenceManager.validate(command.symbol(), command.sequence())
                == SequenceValidation.DUPLICATE) {
            return CommandHandlingResult.DUPLICATE;
        }
        walAppender.append(WalRecord.from(command));
        try {
            commandMatchingEngine.execute(command, ExecutionMode.LIVE);
            sequenceManager.advance(command.symbol(), command.sequence());
            return CommandHandlingResult.APPLIED;
        } catch (RuntimeException exception) {
            recoveryRequiredSymbols.add(command.symbol());
            throw new CommandRecoveryRequiredException(command.symbol(), exception);
        }
    }

    /**
     * 以恢复语义重放已持久化命令，不重复写 WAL 或推进在线序列。
     *
     * @param command 待重放的撮合命令
     * @return 命令已应用的处理结果
     */
    public CommandHandlingResult replay(MatchingCommand command) {
        MatchingCommand matchingCommand = Objects.requireNonNull(command, "撮合命令不能为空");
        synchronized (lockOf(matchingCommand.symbol())) {
            commandMatchingEngine.execute(matchingCommand, ExecutionMode.REPLAY);
            return CommandHandlingResult.APPLIED;
        }
    }

    /**
     * 标记交易对正在恢复，阻止新的在线命令进入。
     *
     * @param symbol 正在恢复的交易对
     */
    void markRecoveryStarted(String symbol) {
        String matchingSymbol = Objects.requireNonNull(symbol, "交易对不能为空");
        synchronized (lockOf(matchingSymbol)) {
            recoveryRequiredSymbols.add(matchingSymbol);
        }
    }

    /**
     * 标记交易对已成功恢复，允许新的在线命令进入。
     *
     * @param symbol 已恢复完成的交易对
     */
    void markRecoveryCompleted(String symbol) {
        String matchingSymbol = Objects.requireNonNull(symbol, "交易对不能为空");
        synchronized (lockOf(matchingSymbol)) {
            recoveryRequiredSymbols.remove(matchingSymbol);
        }
    }

    /**
     * 获取交易对独占处理锁。
     *
     * 锁仅保护同一交易对的“序列校验至序列推进”完整临界区，不同交易对仍可并行处理；
     * 这样可防止两个调用者同时接受同一序列而产生重复 WAL，代价是同一交易对必须串行执行。
     *
     * @param symbol 交易对
     * @return 交易对对应的独占锁对象
     */
    private Object lockOf(String symbol) {
        return symbolLocks.computeIfAbsent(symbol, ignored -> new Object());
    }

    /**
     * 拒绝继续处理存在已持久化未完成命令的交易对。
     *
     * @param symbol 待处理交易对
     */
    private void ensureRecoveryNotRequired(String symbol) {
        if (recoveryRequiredSymbols.contains(symbol)) {
            throw new CommandRecoveryRequiredException(symbol);
        }
    }
}
