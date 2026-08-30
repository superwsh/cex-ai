package com.cex.matching.infrastructure.snapshot;

/** 快照编解码、读写或原子替换失败时抛出的异常。 */
public final class SnapshotException extends RuntimeException {
    /**
     * 创建快照异常。
     *
     * @param message 异常说明
     * @param cause 底层异常
     */
    public SnapshotException(String message, Throwable cause) { super(message, cause); }
}
