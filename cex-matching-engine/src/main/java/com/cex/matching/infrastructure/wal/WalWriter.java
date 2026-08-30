package com.cex.matching.infrastructure.wal;

/** WAL 文件写入契约。 */
public interface WalWriter extends AutoCloseable {

    /**
     * 追加一条 WAL 记录并按实现约定持久化。
     *
     * @param record 待追加的 WAL 记录
     */
    void append(WalRecord record);

    /** 强制持久化已追加的 WAL 数据。 */
    void flush();

    /**
     * 返回当前成功持久化的文件字节数。
     *
     * @return 已持久化字节数
     */
    long writtenBytes();

    /** 关闭 WAL 写入器并释放底层资源。 */
    @Override
    void close();
}
