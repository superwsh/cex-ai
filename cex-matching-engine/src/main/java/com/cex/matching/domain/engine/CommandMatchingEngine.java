package com.cex.matching.domain.engine;

import com.cex.matching.domain.model.MatchingCommand;

/** 将可靠撮合命令转换为确定性内存状态变更的领域接口。 */
public interface CommandMatchingEngine {

    /**
     * 执行一条已通过应用层控制的撮合命令。
     *
     * @param command 待执行的撮合命令
     * @param executionMode 命令执行模式
     */
    void execute(MatchingCommand command, ExecutionMode executionMode);
}
