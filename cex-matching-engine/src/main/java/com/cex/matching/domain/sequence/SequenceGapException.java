package com.cex.matching.domain.sequence;

/** 收到非连续撮合命令时抛出，调用方必须暂停对应分区。 */
public final class SequenceGapException extends IllegalStateException {

    private final String symbol;
    private final long expected;
    private final long actual;

    public SequenceGapException(String symbol, long expected, long actual) {
        super("撮合命令序号存在缺口: symbol=" + symbol + ", expected=" + expected + ", actual=" + actual);
        this.symbol = symbol;
        this.expected = expected;
        this.actual = actual;
    }

    public String getSymbol() { return symbol; }
    public long getExpected() { return expected; }
    public long getActual() { return actual; }
}
