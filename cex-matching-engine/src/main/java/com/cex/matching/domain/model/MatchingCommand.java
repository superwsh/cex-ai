package com.cex.matching.domain.model;

/** 不可变的撮合命令模型。 */
public record MatchingCommand(
        long sequence,
        String commandId,
        String orderId,
        String userId,
        String symbol,
        CommandType commandType,
        MatchOrder.Side side,
        long price,
        long quantity,
        long timestamp) {

    public MatchingCommand {
        if (sequence < 1 || timestamp < 0 || blank(commandId) || blank(orderId)
                || blank(userId) || blank(symbol) || commandType == null) {
            throw new IllegalArgumentException("撮合命令字段不合法");
        }
        if (commandType == CommandType.NEW_ORDER
                && (side == null || price <= 0 || quantity <= 0)) {
            throw new IllegalArgumentException("新订单价格、数量和方向必须有效");
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
