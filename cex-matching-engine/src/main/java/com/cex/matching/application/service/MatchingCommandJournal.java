package com.cex.matching.application.service;

import com.cex.common.kafka.event.OrderEvent;

import java.util.List;
import java.util.Set;

/** 撮合命令的预写日志抽象，隔离核心撮合逻辑与具体持久化介质。 */
public interface MatchingCommandJournal {

    /**
     * 在订单簿变更前追加一条命令。
     *
     * @param symbol 命令所属交易对
     * @param sequence 交易对内连续命令序号
     * @param event 原始订单事件
     */
    void append(String symbol, long sequence, OrderEvent event);

    /**
     * 读取快照序号之后的全部命令，返回顺序必须与 WAL 写入顺序一致。
     *
     * @param symbol 需要恢复的交易对
     * @param sequence 排除该序号及更早的命令
     * @return 待重放的命令列表
     */
    List<RecordedMatchingCommand> readAfter(String symbol, long sequence);

    /**
     * 返回 WAL 中存在命令记录的所有交易对。
     *
     * @return 可在启动时恢复的交易对集合
     */
    Set<String> symbols();

    /**
     * 在快照成功持久化后删除指定序号及更早的命令。
     *
     * @param symbol 需要裁剪日志的交易对
     * @param inclusiveSequence 已由快照覆盖的最大命令序号
     */
    void compact(String symbol, long inclusiveSequence);
}
