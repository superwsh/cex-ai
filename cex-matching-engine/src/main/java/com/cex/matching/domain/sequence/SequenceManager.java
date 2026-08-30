package com.cex.matching.domain.sequence;

public interface SequenceManager {

    long current(String symbol);

    SequenceValidation validate(String symbol, long sequence);

    void advance(String symbol, long sequence);
}
