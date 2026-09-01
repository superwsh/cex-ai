package com.cex.market.domain.exception;

/** 行情盘口事件序号不连续，必须停止应用增量并进入恢复流程。 */
public class MarketSequenceGapException extends RuntimeException {

    /**
     * 创建包含本地和远端序号上下文的异常。
     *
     * @param symbol 发生缺口的交易对
     * @param localSequence 本地已应用的最后序号
     * @param previousSequence 入站事件声明的前序号
     * @param sequence 入站事件序号
     */
    public MarketSequenceGapException(String symbol, long localSequence, long previousSequence, long sequence) {
        super("行情盘口序号不连续: symbol=" + symbol + ", localSequence=" + localSequence
                + ", incomingPreviousSequence=" + previousSequence + ", incomingSequence=" + sequence);
    }
}
