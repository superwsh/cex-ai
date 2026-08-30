package com.cex.matching.infrastructure.wal;

import com.cex.matching.domain.model.MatchingCommand;
import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;

/** 不可变的 WAL 记录。 */
public record WalRecord(
        long sequence,
        String commandId,
        String orderId,
        String userId,
        String symbol,
        CommandType commandType,
        MatchOrder.Side side,
        long price,
        long quantity,
        long timestamp,
        long checksum) {

    public static WalRecord from(MatchingCommand command) {
        return new WalRecord(command.sequence(), command.commandId(), command.orderId(),
                command.userId(), command.symbol(), command.commandType(), command.side(), command.price(),
                command.quantity(), command.timestamp(), 0L);
    }

    public WalRecord withChecksum(long value) {
        return new WalRecord(sequence, commandId, orderId, userId, symbol, commandType,
                side, price, quantity, timestamp, value);
    }
}
