package com.cex.matching.application;

/** 表示交易对在 WAL 已持久化后未完成内存处理，必须先完成恢复。 */
public final class CommandRecoveryRequiredException extends RuntimeException {

    /**
     * 创建被熔断交易对的恢复异常。
     *
     * @param symbol 需要恢复的交易对
     */
    public CommandRecoveryRequiredException(String symbol) {
        super("交易对 " + symbol + " 存在未完成的持久化命令，必须先恢复");
    }

    /**
     * 创建带有原始处理失败原因的恢复异常。
     *
     * @param symbol 需要恢复的交易对
     * @param cause 导致命令未完成的原始异常
     */
    public CommandRecoveryRequiredException(String symbol, RuntimeException cause) {
        super("交易对 " + symbol + " 的持久化命令未完成，必须先恢复", cause);
    }
}
