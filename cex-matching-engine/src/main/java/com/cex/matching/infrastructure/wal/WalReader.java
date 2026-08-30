package com.cex.matching.infrastructure.wal;

/** 按交易对读取 WAL 记录的契约。 */
public interface WalReader {

    /**
     * 读取指定交易对的全部有效 WAL 记录。
     *
     * @param symbol 规范交易对
     * @return 有效记录和不完整尾部状态
     */
    WalReadResult read(String symbol);
}
