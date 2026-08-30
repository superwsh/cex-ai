package com.cex.matching.infrastructure.wal;

/** WAL 内容损坏或格式不合法时抛出的异常。 */
public class WalCorruptionException extends RuntimeException {

    /**
     * 创建不带底层原因的 WAL 损坏异常。
     *
     * @param message 损坏说明
     */
    public WalCorruptionException(String message) {
        super(message);
    }

    /**
     * 创建带底层原因的 WAL 损坏异常。
     *
     * @param message 损坏说明
     * @param cause 底层异常
     */
    public WalCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
