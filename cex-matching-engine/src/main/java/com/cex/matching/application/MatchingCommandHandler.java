package com.cex.matching.application;

import com.cex.matching.domain.engine.CommandMatchingEngine;
import com.cex.matching.domain.engine.ExecutionMode;
import com.cex.matching.domain.model.MatchingCommand;
import com.cex.matching.domain.sequence.SequenceManager;
import com.cex.matching.domain.sequence.SequenceValidation;
import com.cex.matching.infrastructure.wal.WalAppender;
import com.cex.matching.infrastructure.wal.WalRecord;

import java.util.Objects;

/**
 * 编排可靠撮合命令处理的应用服务。
 *
 * LIVE 模式严格执行序列校验、WAL 同步落盘、领域状态变更和序列推进；REPLAY 模式仅重放领域状态。
 */
public final class MatchingCommandHandler {

    private final SequenceManager sequenceManager;
    private final WalAppender walAppender;
    private final CommandMatchingEngine commandMatchingEngine;

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
        if (sequenceManager.validate(matchingCommand.symbol(), matchingCommand.sequence())
                == SequenceValidation.DUPLICATE) {
            return CommandHandlingResult.DUPLICATE;
        }
        walAppender.append(WalRecord.from(matchingCommand));
        commandMatchingEngine.execute(matchingCommand, ExecutionMode.LIVE);
        sequenceManager.advance(matchingCommand.symbol(), matchingCommand.sequence());
        return CommandHandlingResult.APPLIED;
    }

    /**
     * 以恢复语义重放已持久化命令，不重复写 WAL 或推进在线序列。
     *
     * @param command 待重放的撮合命令
     * @return 命令已应用的处理结果
     */
    public CommandHandlingResult replay(MatchingCommand command) {
        commandMatchingEngine.execute(Objects.requireNonNull(command, "撮合命令不能为空"), ExecutionMode.REPLAY);
        return CommandHandlingResult.APPLIED;
    }
}
