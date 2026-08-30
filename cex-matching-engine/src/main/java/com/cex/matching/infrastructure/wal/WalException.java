package com.cex.matching.infrastructure.wal;

/** WAL 写入或生命周期操作失败。 */
public class WalException extends RuntimeException {
    public WalException(String message) {
        super(message);
    }

    public WalException(String message, Throwable cause) {
        super(message, cause);
    }
}
