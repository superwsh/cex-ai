package com.cex.matching.domain.sequence;

public interface SequenceManager {

    long current(String symbol);

    SequenceValidation validate(String symbol, long sequence);

    void advance(String symbol, long sequence);

    /**
     * 使用已持久化状态恢复交易对序列水位。
     *
     * @param symbol 交易对
     * @param sequence 已恢复的最后序列
     */
    void restore(String symbol, long sequence);
}
