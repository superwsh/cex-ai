package com.cex.matching.application.command;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.matching.domain.model.MatchResult;
import com.cex.matching.domain.model.OrderBook;
import com.cex.matching.domain.model.OrderBookSnapshot;
import com.cex.matching.domain.model.RestingOrderSnapshot;
import com.cex.matching.domain.service.InMemoryMatchingEngine;
import com.cex.matching.application.mapper.OrderEventMapper;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** 单个交易对的命令入口，按 Kafka 分区顺序驱动独占订单簿。 */
public final class SymbolMatchingEngine {

    private final String symbol;
    private final OrderEventMapper orderEventMapper;
    private final OrderBook orderBook;
    private final InMemoryMatchingEngine matchingEngine;
    private final AtomicLong commandSequence = new AtomicLong();

    /**
     * 创建单交易对撮合入口。
     *
     * @param symbol 此实例唯一负责的交易对
     * @param orderEventMapper 消息到领域订单的映射器
     * @param tradeIdGenerator 全局唯一的成交编号生成器
     */
    public SymbolMatchingEngine(String symbol, OrderEventMapper orderEventMapper,
                                LongSupplier tradeIdGenerator) {
        this(symbol, orderEventMapper, tradeIdGenerator, new OrderBook(symbol), 0L);
    }

    /**
     * 从订单簿快照恢复单交易对撮合实例。
     *
     * @param snapshot 已校验的订单簿快照
     * @param orderEventMapper 消息到领域订单的映射器
     * @param tradeIdGenerator 成交编号生成器
     * @return 已恢复序号与挂单状态的撮合实例
     */
    public static SymbolMatchingEngine restore(OrderBookSnapshot snapshot, OrderEventMapper orderEventMapper,
                                               LongSupplier tradeIdGenerator) {
        Objects.requireNonNull(snapshot, "订单簿快照不能为空");
        OrderBook orderBook = new OrderBook(snapshot.symbol());
        for (RestingOrderSnapshot orderSnapshot : snapshot.orders()) {
            orderBook.addOrder(orderSnapshot.toMatchOrder());
        }
        return new SymbolMatchingEngine(snapshot.symbol(), orderEventMapper, tradeIdGenerator,
                orderBook, snapshot.sequence());
    }

    /**
     * 处理一个已按交易对顺序投递的订单事件。
     *
     * @param event Kafka 收到的订单提交或撤销事件
     * @return 订单处理结果
     */
    public MatchResult process(OrderEvent event) {
        return processAtSequence(event, commandSequence.get() + 1);
    }

    /**
     * 按指定连续序号处理订单事件，供 WAL 重放和实时命令共用。
     *
     * @param event Kafka 收到的订单提交或撤销事件
     * @param sequence 本命令必须使用的连续序号
     * @return 订单处理结果
     */
    public MatchResult processAtSequence(OrderEvent event, long sequence) {
        validateSequence(sequence);
        validateSymbol(event);
        MatchResult result;
        if (event.getAction() == OrderEvent.Action.SUBMIT) {
            result = matchingEngine.process(orderEventMapper.toMatchOrder(event, sequence));
        } else if (event.getAction() == OrderEvent.Action.CANCEL) {
            result = matchingEngine.cancel(orderEventMapper.parseOrderId(event.getOrderId()), sequence,
                    Instant.ofEpochMilli(Objects.requireNonNull(event.getTimestamp(), "事件时间不能为空")));
        } else {
            throw new IllegalArgumentException("不支持的订单事件动作: " + event.getAction());
        }
        commandSequence.set(sequence);
        return result;
    }

    /**
     * 创建当前订单簿的恢复快照。
     *
     * @return 包含当前序号和全部挂单的不可变快照
     */
    public OrderBookSnapshot snapshot() {
        return new OrderBookSnapshot(symbol, commandSequence.get(),
                orderBook.getRestingOrders().stream().map(RestingOrderSnapshot::from).toList());
    }

    /**
     * 获取下一条命令应使用的连续序号。
     *
     * @return 当前序号加一
     */
    public long nextSequence() {
        return commandSequence.get() + 1;
    }

    /**
     * 获取当前交易对订单簿内的活动订单数量。
     *
     * @return 可成交或可撤销的订单数量
     */
    public int activeOrderCount() {
        return orderBook.getActiveOrderCount();
    }

    /**
     * 校验事件及交易对归属，阻止错误消息污染订单簿。
     *
     * @param event 待处理订单事件
     */
    private void validateSymbol(OrderEvent event) {
        Objects.requireNonNull(event, "订单事件不能为空");
        if (!symbol.equals(event.getSymbol())) {
            throw new IllegalArgumentException("订单事件交易对与撮合实例不一致");
        }
        Objects.requireNonNull(event.getAction(), "订单事件动作不能为空");
    }

    /**
     * 校验新命令的序号必须紧接当前序号，防止 WAL 重放跳号。
     *
     * @param sequence 待处理命令序号
     */
    private void validateSequence(long sequence) {
        long expectedSequence = commandSequence.get() + 1;
        if (sequence != expectedSequence) {
            throw new IllegalStateException("撮合命令序号不连续，期望=" + expectedSequence + "，实际=" + sequence);
        }
    }

    /**
     * 使用指定热状态创建撮合实例。
     *
     * @param symbol 实例负责的交易对
     * @param orderEventMapper 消息到领域订单的映射器
     * @param tradeIdGenerator 成交编号生成器
     * @param orderBook 已创建或已恢复的订单簿
     * @param initialSequence 快照保存的最后命令序号
     */
    private SymbolMatchingEngine(String symbol, OrderEventMapper orderEventMapper,
                                 LongSupplier tradeIdGenerator, OrderBook orderBook, long initialSequence) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        this.symbol = symbol;
        this.orderEventMapper = Objects.requireNonNull(orderEventMapper, "订单事件映射器不能为空");
        this.orderBook = Objects.requireNonNull(orderBook, "订单簿不能为空");
        this.matchingEngine = new InMemoryMatchingEngine(this.orderBook,
                Objects.requireNonNull(tradeIdGenerator, "成交编号生成器不能为空"));
        this.commandSequence.set(initialSequence);
    }
}
