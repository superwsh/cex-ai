package com.cex.matching.application.command;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.common.kafka.event.TradeEvent;
import com.cex.matching.core.MatchingEngine;
import com.cex.matching.application.mapper.OrderResultEventMapper;
import com.cex.matching.application.mapper.TradeEventMapper;
import com.cex.matching.domain.model.MatchResult;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

/** 将订单消息接入撮合注册表，并通过回调输出公共成交事件的应用服务。 */
@Component
@RequiredArgsConstructor
public class KafkaMatchingEngine implements MatchingEngine {

    private final MatchingEngineRegistry matchingEngineRegistry;
    private final TradeEventMapper tradeEventMapper = new TradeEventMapper();
    private final OrderResultEventMapper orderResultEventMapper = new OrderResultEventMapper();

    /**
     * 撮合订单事件，并将本次或重试时缓存的成交事件逐笔交给调用方。
     *
     * @param event 按交易对顺序收到的订单提交或撤销事件
     * @param onTrade 成交事件输出回调
     */
    @Override
    public void match(OrderEvent event, Consumer<TradeEvent> onTrade) {
        matchingEngineRegistry.process(event).ifPresent(result -> publishTrades(result, onTrade));
    }

    @Override
    public void match(OrderEvent event, Consumer<TradeEvent> onTrade,
                      Consumer<com.cex.common.kafka.event.OrderResultEvent> onOrderResult) {
        matchingEngineRegistry.process(event).ifPresent(result -> {
            publishTrades(result, onTrade);
            result.getEvents().stream().map(orderResultEventMapper::toOrderResultEvent)
                    .filter(java.util.Objects::nonNull).forEach(onOrderResult);
        });
    }

    /**
     * 按撮合生成顺序转换并输出成交事件。
     *
     * @param result 撮合命令的处理结果
     * @param onTrade 成交事件输出回调
     */
    private void publishTrades(MatchResult result, Consumer<TradeEvent> onTrade) {
        for (var trade : result.getTrades()) {
            onTrade.accept(tradeEventMapper.toTradeEvent(trade));
        }
    }
}
