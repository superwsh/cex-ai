package com.cex.matching.infrastructure.wal;

import java.util.List;

/**
 * WAL 有效记录与可忽略尾部状态。
 *
 * @param records 已校验的有效记录
 * @param incompleteTail 是否遇到不完整或损坏的最后一条物理记录
 */
public record WalReadResult(List<WalRecord> records, boolean incompleteTail) {

    /**
     * 创建包含不可变记录列表的读取结果。
     *
     * @param records 已校验的有效记录
     * @param incompleteTail 是否遇到可忽略尾部
     */
    public WalReadResult {
        records = List.copyOf(records);
    }
}
