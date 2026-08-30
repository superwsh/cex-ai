package com.cex.matching.domain.sequence;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class InMemorySequenceManager implements SequenceManager {

    private final ConcurrentHashMap<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    @Override
    public long current(String symbol) {
        return sequenceOf(symbol).get();
    }

    @Override
    public SequenceValidation validate(String symbol, long sequence) {
        long current = current(symbol);
        if (sequence <= current) {
            return SequenceValidation.DUPLICATE;
        }
        if (sequence == current + 1L) {
            return SequenceValidation.ACCEPTED;
        }
        throw new SequenceGapException(symbol, current + 1L, sequence);
    }

    @Override
    public void advance(String symbol, long sequence) {
        AtomicLong current = sequenceOf(symbol);
        if (!current.compareAndSet(sequence - 1L, sequence)) {
            throw new IllegalStateException("交易对 " + symbol + " 的序列号无法推进到 " + sequence);
        }
    }

    private AtomicLong sequenceOf(String symbol) {
        return sequences.computeIfAbsent(symbol, ignored -> new AtomicLong());
    }
}
