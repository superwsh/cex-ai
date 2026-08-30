package com.cex.matching.infrastructure.wal;

/** WAL 写入或生命周期操作失败。 */
public class WalException extends RuntimeException {

    /**
     * 创建不带底层原因的 WAL 异常。
     *
     * @param message 异常说明
     */
    public WalException(String message) {
        super(message);
    }

    /**
     * 创建带底层原因的 WAL 异常。
     *
     * @param message 异常说明
     * @param cause 底层异常
     */
    public WalException(String message, Throwable cause) {
        super(message, cause);
    }
}
