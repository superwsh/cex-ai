package com.cex.matching.infrastructure.wal;

import java.util.List;

public record WalReadResult(List<WalRecord> records, boolean incompleteTail) {
    public WalReadResult {
        records = List.copyOf(records);
    }
}
