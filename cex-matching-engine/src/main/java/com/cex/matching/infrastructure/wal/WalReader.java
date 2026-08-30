package com.cex.matching.infrastructure.wal;

public interface WalReader {
    WalReadResult read(String symbol);
}
