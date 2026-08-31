package com.cex.matching.application.service;

import com.cex.common.kafka.event.OrderEvent;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** 仅供单元测试使用的内存 WAL，实现与文件 WAL 相同的序号校验语义。 */
public final class InMemoryMatchingCommandJournal implements MatchingCommandJournal {

    private final ConcurrentHashMap<String, List<RecordedMatchingCommand>> commandsBySymbol = new ConcurrentHashMap<>();

    /**
     * 追加指定交易对的命令，并校验该交易对内序号连续。
     *
     * @param symbol 交易对
     * @param sequence 待写入序号
     * @param event 原始订单事件
     */
    @Override
    public void append(String symbol, long sequence, OrderEvent event) {
        List<RecordedMatchingCommand> commands = commandsBySymbol.computeIfAbsent(symbol, key -> new java.util.ArrayList<>());
        long expectedSequence = commands.stream().mapToLong(RecordedMatchingCommand::sequence).max().orElse(0L) + 1L;
        if (sequence != expectedSequence) {
            throw new IllegalStateException("内存 WAL 命令序号不连续，期望=" + expectedSequence + "，实际=" + sequence);
        }
        commands.add(new RecordedMatchingCommand(symbol, sequence, event));
    }

    /**
     * 读取一个交易对中指定序号之后的命令。
     *
     * @param symbol 交易对
     * @param sequence 排除该序号及更早的命令
     * @return 待重放的命令记录
     */
    @Override
    public List<RecordedMatchingCommand> readAfter(String symbol, long sequence) {
        return commandsBySymbol.getOrDefault(symbol, List.of()).stream()
                .filter(command -> command.sequence() > sequence).toList();
    }

    /**
     * 获取已经写入命令的全部交易对。
     *
     * @return 交易对集合
     */
    @Override
    public Set<String> symbols() {
        return Set.copyOf(commandsBySymbol.keySet());
    }

    /**
     * 删除已被快照覆盖的指定交易对命令。
     *
     * @param symbol 交易对
     * @param inclusiveSequence 可删除的最大序号（含）
     */
    @Override
    public void compact(String symbol, long inclusiveSequence) {
        commandsBySymbol.getOrDefault(symbol, List.of()).removeIf(command -> command.sequence() <= inclusiveSequence);
    }
}
