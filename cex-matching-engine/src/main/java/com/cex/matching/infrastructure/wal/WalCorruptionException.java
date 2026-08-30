package com.cex.matching.infrastructure.wal;

/** WAL 内容损坏或格式不合法时抛出的异常。 */
public class WalCorruptionException extends RuntimeException {
    public WalCorruptionException(String message) {
        super(message);
    }

    public WalCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
