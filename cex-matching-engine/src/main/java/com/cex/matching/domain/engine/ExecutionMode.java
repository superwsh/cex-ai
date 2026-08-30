package com.cex.matching.domain.engine;

/** 命令进入内存撮合领域时的执行模式。 */
public enum ExecutionMode {
    /** 正常在线处理命令。 */
    LIVE,
    /** 从已持久化日志重放命令。 */
    REPLAY
}
