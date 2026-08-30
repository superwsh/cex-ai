package com.cex.matching.domain.engine;

import com.cex.matching.application.CommandNormalizer;
import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.model.MatchingCommand;
import com.cex.matching.domain.model.OrderBook;
import com.cex.matching.domain.snapshot.MatchingSnapshot;
import com.cex.matching.domain.snapshot.SnapshotOrder;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于内存订单簿的命令执行器。
 *
 * 该类不保证线程安全，同一交易对的命令必须由上层单线程串行调用。
 */
public final class InMemoryCommandMatchingEngine implements CommandMatchingEngine {

    private final CommandNormalizer commandNormalizer;
    private final Map<String, OrderBook> orderBooks = new ConcurrentHashMap<>();

    /**
     * 创建内存命令执行器。
     *
     * @param commandNormalizer 整数金额到内部订单的归一化器
     */
    public InMemoryCommandMatchingEngine(CommandNormalizer commandNormalizer) {
        this.commandNormalizer = Objects.requireNonNull(commandNormalizer, "命令归一化器不能为空");
    }

    /**
     * 对对应交易对订单簿执行新增或取消命令。
     *
     * @param command 已通过应用层处理的撮合命令
     * @param executionMode 当前执行模式，仅用于显式表达在线与重放语义
     */
    @Override
    public void execute(MatchingCommand command, ExecutionMode executionMode) {
        MatchingCommand matchingCommand = Objects.requireNonNull(command, "撮合命令不能为空");
        Objects.requireNonNull(executionMode, "执行模式不能为空");
        switch (matchingCommand.commandType()) {
            case NEW_ORDER -> addOrder(matchingCommand);
            case CANCEL_ORDER -> cancelOrder(matchingCommand);
        }
    }

    /**
     * 查询指定交易对中仍处于活动状态的订单。
     *
     * @param symbol 交易对
     * @param orderId 数值订单编号
     * @return 活动订单；订单簿或订单不存在时为空
     */
    public Optional<MatchOrder> findOrder(String symbol, long orderId) {
        OrderBook orderBook = orderBooks.get(symbol);
        return orderBook == null ? Optional.empty() : orderBook.getOrder(orderId);
    }

    /**
     * 使用快照完整替换一个交易对的活动订单簿。
     *
     * @param snapshot 已校验的交易对快照
     */
    public void restore(MatchingSnapshot snapshot) {
        MatchingSnapshot matchingSnapshot = Objects.requireNonNull(snapshot, "撮合快照不能为空");
        OrderBook restoredBook = new OrderBook(matchingSnapshot.symbol());
        matchingSnapshot.buyOrders().forEach(order -> addSnapshotOrder(restoredBook, matchingSnapshot.symbol(), order));
        matchingSnapshot.sellOrders().forEach(order -> addSnapshotOrder(restoredBook, matchingSnapshot.symbol(), order));
        orderBooks.put(matchingSnapshot.symbol(), restoredBook);
    }

    /**
     * 清空指定交易对的内存订单簿，为恢复过程建立空基线。
     *
     * @param symbol 待清空的交易对
     */
    public void reset(String symbol) {
        String matchingSymbol = Objects.requireNonNull(symbol, "交易对不能为空");
        orderBooks.put(matchingSymbol, new OrderBook(matchingSymbol));
    }

    /**
     * 将快照订单还原为内存订单并加入目标订单簿。
     *
     * @param orderBook 待恢复订单簿
     * @param symbol 交易对
     * @param order 快照订单
     */
    private void addSnapshotOrder(OrderBook orderBook, String symbol, SnapshotOrder order) {
        orderBook.addOrder(new MatchOrder(order.orderId(), order.userId(), symbol, order.side(), order.price(),
                order.quantity(), order.remainingQuantity(), order.sequence()));
    }

    /**
     * 将新增命令归一化为内部订单并加入订单簿。
     *
     * @param command 新订单命令
     */
    private void addOrder(MatchingCommand command) {
        bookOf(command.symbol()).addOrder(commandNormalizer.toMatchOrder(command));
    }

    /**
     * 从订单簿删除目标订单；目标不存在时保持幂等成功。
     *
     * @param command 取消订单命令
     */
    private void cancelOrder(MatchingCommand command) {
        bookOf(command.symbol()).removeOrder(parseOrderId(command.orderId()));
    }

    /**
     * 获取交易对订单簿，不存在时创建空订单簿。
     *
     * @param symbol 交易对
     * @return 对应的内存订单簿
     */
    private OrderBook bookOf(String symbol) {
        return orderBooks.computeIfAbsent(symbol, OrderBook::new);
    }

    /**
     * 将命令中的订单编号转换为内部数值编号。
     *
     * @param orderId 命令订单编号
     * @return 数值订单编号
     */
    private long parseOrderId(String orderId) {
        try {
            return Long.parseLong(orderId);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("命令订单编号不能转换为 long", exception);
        }
    }
}
