package com.cex.matching.infrastructure.wal;

/** WAL 文件写入契约。 */
public interface WalWriter extends AutoCloseable {
    void append(WalRecord record);

    void flush();

    long writtenBytes();

    @Override
    void close();
}
