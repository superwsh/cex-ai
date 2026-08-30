package com.cex.matching.infrastructure.wal;

/** 为应用层提供追加已归一化 WAL 记录的基础设施端口。 */
@FunctionalInterface
public interface WalAppender {

    /**
     * 同步追加一条 WAL 记录；成功返回时记录必须已由具体实现持久化。
     *
     * @param record 待追加的 WAL 记录
     */
    void append(WalRecord record);
}
