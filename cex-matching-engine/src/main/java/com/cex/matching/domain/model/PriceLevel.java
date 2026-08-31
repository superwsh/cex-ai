package com.cex.matching.domain.model;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 单个价格档位，以先进先出队列保留同价订单的到达顺序。
 * 该对象只应由所属的撮合线程访问。
 */
public final class PriceLevel {

    private final BigDecimal price;
    private final Deque<MatchOrder> orders = new ArrayDeque<>();

    /** 创建指定价格的订单档位。 */
    public PriceLevel(BigDecimal price) {
        if (price == null || price.signum() <= 0) {
            throw new IllegalArgumentException("price must be positive");
        }
        this.price = price;
    }

    public BigDecimal getPrice() {
        return price;
    }

    /** 将同价订单追加至队尾以保证时间优先。 */
    public void addOrder(MatchOrder order) {
        Objects.requireNonNull(order, "order must not be null");
        if (price.compareTo(order.getPrice()) != 0) {
            throw new IllegalArgumentException("order price does not match price level");
        }
        orders.addLast(order);
    }

    /** 删除指定订单编号，供订单索引直接定位后调用。 */
    public boolean removeOrder(long orderId) {
        Iterator<MatchOrder> iterator = orders.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getOrderId() == orderId) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    /** 返回队首挂单，即该价格档位最早进入的订单。 */
    public MatchOrder getFirstOrder() {
        return orders.peekFirst();
    }

    /** 兼容旧撮合 API。 */
    public MatchOrder peekFirst() {
        return getFirstOrder();
    }

    public boolean isEmpty() {
        return orders.isEmpty();
    }

    public int size() {
        return orders.size();
    }

    /** 返回该价格档位全部订单的剩余数量。 */
    public BigDecimal getTotalRemainingQuantity() {
        return orders.stream()
                .map(MatchOrder::getRemainingQuantity)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    /** 兼容旧撮合 API。 */
    public BigDecimal totalRemainingQuantity() {
        return getTotalRemainingQuantity();
    }

    /** 判断指定订单是否仍在当前价格档位。 */
    public boolean contains(MatchOrder order) {
        return orders.contains(order);
    }

    public List<MatchOrder> getOrders() {
        return List.copyOf(orders);
    }
}
