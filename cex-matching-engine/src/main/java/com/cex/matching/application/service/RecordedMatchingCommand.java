package com.cex.matching.application.service;

import com.cex.common.kafka.event.OrderEvent;

import java.util.Objects;

/** 写入 WAL 的单条撮合命令，序号仅在所属交易对内连续。 */
public record RecordedMatchingCommand(String symbol, long sequence, OrderEvent event) {

    /** 校验 WAL 命令的路由键、连续序号和事件一致性。 */
    public RecordedMatchingCommand {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("WAL 交易对不能为空");
        }
        if (sequence <= 0) {
            throw new IllegalArgumentException("WAL 命令序号必须大于零");
        }
        event = Objects.requireNonNull(event, "WAL 订单事件不能为空");
        if (!symbol.equals(event.getSymbol())) {
            throw new IllegalArgumentException("WAL 交易对与订单事件不一致");
        }
    }
}
