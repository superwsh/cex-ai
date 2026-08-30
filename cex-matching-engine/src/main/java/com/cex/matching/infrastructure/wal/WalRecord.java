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

    /**
     * 从撮合命令创建尚未计算校验和的 WAL 记录。
     *
     * @param command 撮合命令
     * @return 字段与命令一致的 WAL 记录
     */
    public static WalRecord from(MatchingCommand command) {
        return new WalRecord(command.sequence(), command.commandId(), command.orderId(),
                command.userId(), command.symbol(), command.commandType(), command.side(), command.price(),
                command.quantity(), command.timestamp(), 0L);
    }

    /**
     * 复制当前记录并替换校验和值。
     *
     * @param value 已计算的 CRC32 校验和
     * @return 带指定校验和的新记录
     */
    public WalRecord withChecksum(long value) {
        return new WalRecord(sequence, commandId, orderId, userId, symbol, commandType,
                side, price, quantity, timestamp, value);
    }
}
