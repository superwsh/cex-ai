package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.enums.OrderType;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 单交易对的内存订单簿。
 *
 * <p>该类刻意不提供线程安全保证。后续由命令队列和单一撮合线程独占所有修改，
 * 以保证确定性的价格优先、时间优先顺序。</p>
 */
public final class OrderBook {
    private final String symbol;
    private final NavigableMap<BigDecimal, PriceLevel> bids =
            new TreeMap<>(Comparator.reverseOrder());
    private final NavigableMap<BigDecimal, PriceLevel> asks = new TreeMap<>();
    private final Map<Long, OrderLocation> orderIndex = new LinkedHashMap<>();

    /**
     * 创建单交易对订单簿。
     *
     * @param symbol 订单簿所属交易对
     */
    public OrderBook(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol must not be blank");
        }
        this.symbol = symbol;
    }

    /**
     * 将未成交限价单加入对应价格档位和订单索引。
     *
     * @param order 需要挂入订单簿的限价单
     */
    public void addOrder(MatchOrder order) {
        validateRestingOrder(order);
        if (orderIndex.containsKey(order.getOrderId())) {
            throw new IllegalArgumentException("duplicate orderId: " + order.getOrderId());
        }

        NavigableMap<BigDecimal, PriceLevel> levels = levelsFor(order.getSide());
        PriceLevel level = levels.computeIfAbsent(order.getPrice(), PriceLevel::new);
        level.addOrder(order);
        orderIndex.put(order.getOrderId(), new OrderLocation(
                symbol, order.getSide(), order.getPrice(), level, order));
    }

    /**
     * 从价格档位和订单索引中删除指定订单。
     *
     * @param orderId 要删除的订单编号
     * @return 已删除订单；不存在时为空
     */
    public Optional<MatchOrder> removeOrder(long orderId) {
        OrderLocation location = orderIndex.remove(orderId);
        if (location == null) {
            return Optional.empty();
        }

        if (!location.getPriceLevel().removeOrder(orderId)) {
            throw new IllegalStateException("order index and price level are inconsistent: " + orderId);
        }
        if (location.getPriceLevel().isEmpty()) {
            levelsFor(location.getSide()).remove(location.getPrice());
        }
        return Optional.of(location.getOrder());
    }

    /** 返回价格最高的买方价格档位。 */
    public Optional<PriceLevel> getBestBid() {
        return Optional.ofNullable(bids.isEmpty() ? null : bids.firstEntry().getValue());
    }

    /** 返回价格最低的卖方价格档位。 */
    public Optional<PriceLevel> getBestAsk() {
        return Optional.ofNullable(asks.isEmpty() ? null : asks.firstEntry().getValue());
    }

    /** 根据订单索引查询挂单。 */
    public Optional<MatchOrder> getOrder(long orderId) {
        OrderLocation location = orderIndex.get(orderId);
        return location == null ? Optional.empty() : Optional.of(location.getOrder());
    }

    /** 判断订单是否仍在订单簿中。 */
    public boolean containsOrder(long orderId) {
        return orderIndex.containsKey(orderId);
    }

    /** 返回按价格从高到低排列的买方深度。 */
    public Map<BigDecimal, BigDecimal> getBidDepth() {
        return depthOf(bids);
    }

    /** 返回按价格从低到高排列的卖方深度。 */
    public Map<BigDecimal, BigDecimal> getAskDepth() {
        return depthOf(asks);
    }

    public String getSymbol() {
        return symbol;
    }

    /**
     * 获取订单索引中记录的当前活动订单数量。
     *
     * @return 活动订单数量
     */
    public int getActiveOrderCount() {
        return orderIndex.size();
    }

    /**
     * 按买盘后卖盘、各自价格优先与时间优先顺序返回全部挂单。
     *
     * @return 不可修改的订单簿挂单列表
     */
    public java.util.List<MatchOrder> getRestingOrders() {
        java.util.List<MatchOrder> orders = new java.util.ArrayList<>(orderIndex.size());
        appendOrders(bids, orders);
        appendOrders(asks, orders);
        return java.util.List.copyOf(orders);
    }

    private void validateRestingOrder(MatchOrder order) {
        Objects.requireNonNull(order, "order must not be null");
        if (!symbol.equals(order.getSymbol())) {
            throw new IllegalArgumentException("order symbol does not match order book symbol");
        }
        if (order.getType() != OrderType.LIMIT) {
            throw new IllegalArgumentException("only limit orders can rest in an order book");
        }
        if (order.getRemainingQuantity().signum() <= 0) {
            throw new IllegalArgumentException("resting order must have remaining quantity");
        }
    }

    private NavigableMap<BigDecimal, PriceLevel> levelsFor(OrderSide side) {
        return side == OrderSide.BUY ? bids : asks;
    }

    private Map<BigDecimal, BigDecimal> depthOf(NavigableMap<BigDecimal, PriceLevel> levels) {
        Map<BigDecimal, BigDecimal> depth = new LinkedHashMap<>();
        levels.forEach((price, level) -> depth.put(price, level.getTotalRemainingQuantity()));
        return Collections.unmodifiableMap(depth);
    }

    /**
     * 按价格档位和档位内先进先出顺序收集挂单。
     *
     * @param levels 需要遍历的买盘或卖盘价格档位
     * @param orders 用于接收挂单的集合
     */
    private void appendOrders(NavigableMap<BigDecimal, PriceLevel> levels, java.util.List<MatchOrder> orders) {
        for (PriceLevel level : levels.values()) {
            orders.addAll(level.getOrders());
        }
    }
}
