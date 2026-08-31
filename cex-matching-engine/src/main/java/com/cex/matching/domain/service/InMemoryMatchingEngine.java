package com.cex.matching.domain.service;

import com.cex.matching.domain.enums.OrderEventType;
import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.enums.OrderStatus;
import com.cex.matching.domain.enums.OrderType;
import com.cex.matching.domain.enums.TimeInForce;
import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.model.MatchResult;
import com.cex.matching.domain.model.OrderBook;
import com.cex.matching.domain.model.OrderEvent;
import com.cex.matching.domain.model.PriceLevel;
import com.cex.matching.domain.model.Trade;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * 单交易对的内存撮合器。
 * 调用方必须通过同一个撮合线程串行调用本类，本类不增加锁或并发容器。
 */
public final class InMemoryMatchingEngine {

    private final OrderBook orderBook;
    private final LongSupplier tradeIdGenerator;
    private final Map<Long, MatchResult> canceledOrders = new HashMap<>();

    /**
     * 创建内存撮合器。
     *
     * @param orderBook 由该引擎独占修改的订单簿
     * @param tradeIdGenerator 单线程成交编号生成器
     */
    public InMemoryMatchingEngine(OrderBook orderBook, LongSupplier tradeIdGenerator) {
        this.orderBook = Objects.requireNonNull(orderBook, "订单簿不能为空");
        this.tradeIdGenerator = Objects.requireNonNull(tradeIdGenerator, "成交编号生成器不能为空");
    }

    /**
     * 按价格优先、时间优先规则处理订单。
     *
     * @param takerOrder 本次进入撮合器的订单
     * @return 成交、订单状态和领域事件组成的处理结果
     */
    public MatchResult process(MatchOrder takerOrder) {
        validateTakerOrder(takerOrder);
        List<OrderEvent> events = new ArrayList<>();
        emit(events, takerOrder, OrderEventType.ORDER_ACCEPTED, null);
        if (isUnfillableFokOrder(takerOrder)) {
            return cancelOrderAfterFokCheck(takerOrder, events);
        }
        List<Trade> trades = matchOrder(takerOrder, events);
        return completeOrderProcessing(takerOrder, trades, events);
    }

    /**
     * 撤销订单簿内的挂单；已撤销订单重复调用时返回首次撤销结果。
     *
     * @param orderId 要撤销的订单编号
     * @param sequence 撤单命令的全局序号
     * @param timestamp 撤单命令发生时间
     * @return 撤单结果；已成交或不存在的订单返回拒绝结果
     */
    public MatchResult cancel(long orderId, long sequence, Instant timestamp) {
        MatchResult previousCancel = canceledOrders.get(orderId);
        if (previousCancel != null) {
            return previousCancel;
        }
        MatchOrder order = orderBook.removeOrder(orderId).orElse(null);
        if (order == null) {
            return rejectedCancelResult(orderId, sequence, timestamp);
        }
        order.cancel();
        List<OrderEvent> events = new ArrayList<>();
        events.add(createEvent(order, OrderEventType.ORDER_CANCELED, null,
                new EventContext(sequence, timestamp, 0)));
        MatchResult result = new MatchResult(order, List.of(), events);
        canceledOrders.put(orderId, result);
        return result;
    }

    /**
     * 使用固定时间戳撤单，供纯内存测试和顺序回放使用。
     *
     * @param orderId 要撤销的订单编号
     * @param sequence 撤单命令的全局序号
     * @return 撤单结果
     */
    public MatchResult cancel(long orderId, long sequence) {
        return cancel(orderId, sequence, Instant.EPOCH);
    }

    /**
     * 为不存在或已成交订单生成拒绝撤单结果。
     *
     * @param orderId 被拒绝撤单的订单编号
     * @param sequence 撤单命令序号
     * @param timestamp 撤单命令发生时间
     * @return 包含拒绝事件的处理结果
     */
    private MatchResult rejectedCancelResult(long orderId, long sequence, Instant timestamp) {
        String eventId = sequence + ":" + OrderEventType.ORDER_REJECTED + ":" + orderId + ":0";
        OrderEvent event = OrderEvent.builder()
                .eventId(eventId)
                .sequence(sequence)
                .orderId(orderId)
                .symbol(orderBook.getSymbol())
                .timestamp(timestamp)
                .type(OrderEventType.ORDER_REJECTED)
                .build();
        return new MatchResult(orderId, OrderStatus.REJECTED,
                BigDecimal.ZERO, List.of(), List.of(event));
    }

    /**
     * 循环撮合盘口最优的对手方订单，直到当前订单完成或价格不可成交。
     *
     * @param takerOrder 本次进入撮合器的订单
     * @param events 需要按处理顺序追加的领域事件集合
     * @return 本次生成的全部成交记录
     */
    private List<Trade> matchOrder(MatchOrder takerOrder, List<OrderEvent> events) {
        List<Trade> trades = new ArrayList<>();
        while (takerOrder.hasRemainingToMatch()) {
            PriceLevel oppositeLevel = bestOppositeLevel(takerOrder.getSide());
            if (oppositeLevel == null || !canMatch(takerOrder, oppositeLevel)) {
                break;
            }
            MatchOrder makerOrder = oppositeLevel.getFirstOrder();
            Trade trade = createTrade(makerOrder, takerOrder, trades.size());
            if (trade == null) {
                break;
            }
            makerOrder.applyFill(trade.getQuantity(), trade.getQuoteAmount());
            takerOrder.applyFill(trade.getQuantity(), trade.getQuoteAmount());
            trades.add(trade);
            emit(events, takerOrder, OrderEventType.TRADE_CREATED, trade.getTradeId());
            emitMakerStatus(events, makerOrder);
            if (makerOrder.getStatus() == OrderStatus.FILLED) {
                orderBook.removeOrder(makerOrder.getOrderId());
            }
        }
        return trades;
    }

    /**
     * 根据订单有效期处理剩余数量，并输出最终订单状态事件。
     *
     * @param order 已完成撮合循环的订单
     * @param trades 本次已生成的成交记录
     * @param events 需要补充最终状态的领域事件集合
     * @return 最终处理结果
     */
    private MatchResult completeOrderProcessing(MatchOrder order, List<Trade> trades,
                                                List<OrderEvent> events) {
        if (shouldRest(order)) {
            orderBook.addOrder(order);
        } else if (order.hasRemainingToMatch()) {
            order.cancel();
        }
        emitTakerStatus(events, order);
        MatchResult result = new MatchResult(order, trades, events);
        rememberCanceledOrder(result);
        return result;
    }

    /**
     * 在 FOK 流动性预检查失败时取消订单，确保订单簿不发生任何成交修改。
     *
     * @param order 无法完全成交的 FOK 订单
     * @param events 已包含接收事件、需要追加取消事件的集合
     * @return 取消结果
     */
    private MatchResult cancelOrderAfterFokCheck(MatchOrder order, List<OrderEvent> events) {
        order.cancel();
        emit(events, order, OrderEventType.ORDER_CANCELED, null);
        MatchResult result = new MatchResult(order, List.of(), events);
        rememberCanceledOrder(result);
        return result;
    }

    /**
     * 使用挂单价格和双方可成交的最大数量创建一次成交。
     *
     * @param makerOrder 订单簿中先存在的挂单
     * @param takerOrder 当前进入撮合器的订单
     * @return 不可变成交记录；市价买单预算不足一个最小可表示数量时返回 null
     */
    private Trade createTrade(MatchOrder makerOrder, MatchOrder takerOrder, int matchIndex) {
        BigDecimal quantity = calculateTradeQuantity(makerOrder, takerOrder);
        if (quantity.signum() <= 0) {
            return null;
        }
        return Trade.builder()
                .tradeId(deterministicTradeId(makerOrder, takerOrder, matchIndex))
                .symbol(orderBook.getSymbol())
                .makerOrderId(makerOrder.getOrderId())
                .takerOrderId(takerOrder.getOrderId())
                .makerSide(makerOrder.getSide())
                .price(makerOrder.getPrice())
                .quantity(quantity)
                .timestamp(takerOrder.getCreatedAt())
                .sequence(takerOrder.getSequence())
                .build();
    }

    /**
     * 根据订单剩余数量或市价买单预算计算本次可成交的数量。
     * 市价买单按挂单价格换算预算，向下舍入以确保不超支。
     *
     * @param makerOrder 订单簿中先存在的挂单
     * @param takerOrder 当前进入撮合器的订单
     * @return 本次可成交的基础资产数量
     */
    private BigDecimal calculateTradeQuantity(MatchOrder makerOrder, MatchOrder takerOrder) {
        if (takerOrder.getType() != OrderType.MARKET || takerOrder.getSide() != OrderSide.BUY) {
            return makerOrder.getRemainingQuantity().min(takerOrder.getRemainingQuantity());
        }
        int scale = Math.max(makerOrder.getRemainingQuantity().scale(),
                takerOrder.getRemainingQuoteAmount().scale() + makerOrder.getPrice().scale());
        BigDecimal affordableQuantity = takerOrder.getRemainingQuoteAmount()
                .divide(makerOrder.getPrice(), scale, RoundingMode.DOWN);
        return makerOrder.getRemainingQuantity().min(affordableQuantity);
    }

    /**
     * 根据挂单成交后的状态生成成交或部分成交事件。
     *
     * @param events 需要追加事件的集合
     * @param makerOrder 已更新剩余数量的挂单
     */
    private void emitMakerStatus(List<OrderEvent> events, MatchOrder makerOrder) {
        OrderEventType type = makerOrder.getStatus() == OrderStatus.FILLED
                ? OrderEventType.ORDER_FILLED : OrderEventType.ORDER_PARTIALLY_FILLED;
        emit(events, makerOrder, type, null);
    }

    /**
     * 根据当前订单的最终状态生成相应订单事件。
     *
     * @param events 需要追加事件的集合
     * @param takerOrder 已完成当前命令处理的订单
     */
    private void emitTakerStatus(List<OrderEvent> events, MatchOrder takerOrder) {
        if (takerOrder.getStatus() == OrderStatus.FILLED) {
            emit(events, takerOrder, OrderEventType.ORDER_FILLED, null);
        } else if (takerOrder.getStatus() == OrderStatus.PARTIALLY_FILLED) {
            emit(events, takerOrder, OrderEventType.ORDER_PARTIALLY_FILLED, null);
        } else if (takerOrder.getStatus() == OrderStatus.CANCELED) {
            emitPartialFillIfNeeded(events, takerOrder);
            emit(events, takerOrder, OrderEventType.ORDER_CANCELED, null);
        }
    }

    /**
     * 在取消前已发生部分成交时补充部分成交事件。
     *
     * @param events 需要追加事件的集合
     * @param order 已部分成交且即将取消的订单
     */
    private void emitPartialFillIfNeeded(List<OrderEvent> events, MatchOrder order) {
        if (order.hasAnyFill()) {
            emit(events, order, OrderEventType.ORDER_PARTIALLY_FILLED, null);
        }
    }

    /**
     * 校验订单归属、状态和幂等键，防止重复或错误命令改变订单簿。
     *
     * @param takerOrder 待处理订单
     */
    private void validateTakerOrder(MatchOrder takerOrder) {
        Objects.requireNonNull(takerOrder, "订单不能为空");
        if (!orderBook.getSymbol().equals(takerOrder.getSymbol())) {
            throw new IllegalArgumentException("订单交易对与订单簿不一致");
        }
        if (takerOrder.getStatus() != OrderStatus.OPEN) {
            throw new IllegalArgumentException("只有开放状态订单可以进入撮合器");
        }
        if (orderBook.containsOrder(takerOrder.getOrderId()) || canceledOrders.containsKey(takerOrder.getOrderId())) {
            throw new IllegalArgumentException("订单编号已被处理");
        }
    }

    /**
     * 判断 FOK 订单是否因可成交流动性不足而必须直接取消。
     *
     * @param order 待预检查的订单
     * @return 无法完全成交时为 true
     */
    private boolean isUnfillableFokOrder(MatchOrder order) {
        return order.getType() == OrderType.LIMIT
                && order.getTimeInForce() == TimeInForce.FOK
                && !canFullyFill(order);
    }

    /**
     * 按可成交价格档位累加深度，预检查限价订单能否全部成交。
     *
     * @param order 需要检查的 FOK 限价订单
     * @return 可成交深度足够时为 true
     */
    private boolean canFullyFill(MatchOrder order) {
        BigDecimal availableQuantity = BigDecimal.ZERO;
        Map<BigDecimal, BigDecimal> depth = order.getSide() == OrderSide.BUY
                ? orderBook.getAskDepth() : orderBook.getBidDepth();
        for (Map.Entry<BigDecimal, BigDecimal> entry : depth.entrySet()) {
            if (!isExecutablePrice(order, entry.getKey())) {
                break;
            }
            availableQuantity = availableQuantity.add(entry.getValue());
            if (availableQuantity.compareTo(order.getRemainingQuantity()) >= 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取当前订单对手方的最优价格档位。
     *
     * @param takerSide 当前订单买卖方向
     * @return 对手方最优档位；盘口为空时为 null
     */
    private PriceLevel bestOppositeLevel(OrderSide takerSide) {
        return takerSide == OrderSide.BUY
                ? orderBook.getBestAsk().orElse(null)
                : orderBook.getBestBid().orElse(null);
    }

    /**
     * 判断订单与对手方最优档位是否满足成交价格条件。
     *
     * @param takerOrder 当前订单
     * @param oppositeLevel 对手方最优价格档位
     * @return 市价单或价格满足条件时为 true
     */
    private boolean canMatch(MatchOrder takerOrder, PriceLevel oppositeLevel) {
        return takerOrder.getType() == OrderType.MARKET
                || isExecutablePrice(takerOrder, oppositeLevel.getPrice());
    }

    /**
     * 判断限价单价格是否可与指定对手方价格成交。
     *
     * @param order 限价订单
     * @param oppositePrice 对手方价格
     * @return 买价不低于卖价或卖价不高于买价时为 true
     */
    private boolean isExecutablePrice(MatchOrder order, BigDecimal oppositePrice) {
        int comparison = order.getPrice().compareTo(oppositePrice);
        return order.getSide() == OrderSide.BUY ? comparison >= 0 : comparison <= 0;
    }

    /**
     * 判断剩余数量是否应作为 GTC 限价单进入订单簿。
     *
     * @param order 已执行一轮撮合的订单
     * @return 剩余数量需要挂单时为 true
     */
    private boolean shouldRest(MatchOrder order) {
        return order.hasRemainingToMatch()
                && order.getType() == OrderType.LIMIT
                && order.getTimeInForce() == TimeInForce.GTC;
    }

    /**
     * 缓存取消结果，以保证重复撤单返回同一结果而不重复生成事件。
     *
     * @param result 已完成的订单处理结果
     */
    private void rememberCanceledOrder(MatchResult result) {
        if (result.getFinalStatus() == OrderStatus.CANCELED) {
            canceledOrders.put(result.getOrderId(), result);
        }
    }

    /**
     * 从单线程编号生成器获取下一笔成交编号。
     *
     * @return 大于零的成交编号
     */
    private long nextTradeId() {
        long tradeId = tradeIdGenerator.getAsLong();
        if (tradeId <= 0) {
            throw new IllegalStateException("成交编号必须大于零");
        }
        return tradeId;
    }

    /**
     * 以交易对、命令序号和成交位置计算稳定成交编号，重放不依赖进程内计数器。
     *
     * @param makerOrder 挂单方订单
     * @param takerOrder 吃单方订单
     * @param matchIndex 同一命令内的成交序号
     * @return 正数成交编号
     */
    private String deterministicTradeId(MatchOrder makerOrder, MatchOrder takerOrder, int matchIndex) {
        String encodedSymbol = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(orderBook.getSymbol().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return encodedSymbol + '-' + takerOrder.getSequence() + '-' + takerOrder.getOrderId()
                + '-' + makerOrder.getOrderId() + '-' + matchIndex;
    }

    /**
     * 使用订单自身的序号和时间创建并追加领域事件。
     *
     * @param events 需要追加事件的集合
     * @param order 事件关联订单
     * @param type 事件类型
     * @param tradeId 成交事件关联的成交编号；订单事件为 null
     */
    private void emit(List<OrderEvent> events, MatchOrder order, OrderEventType type, String tradeId) {
        EventContext context = new EventContext(order.getSequence(), order.getCreatedAt(), events.size());
        events.add(createEvent(order, type, tradeId, context));
    }

    /**
     * 按确定性规则生成带有唯一事件编号的领域事件。
     *
     * @param order 事件关联订单
     * @param type 事件类型
     * @param tradeId 可选的成交编号
     * @param context 命令序号、时间和同命令内事件偏移量
     * @return 不可变领域事件
     */
    private OrderEvent createEvent(MatchOrder order, OrderEventType type, String tradeId,
                                   EventContext context) {
        String eventId = context.sequence + ":" + type + ":" + order.getOrderId() + ":" + context.offset;
        return OrderEvent.builder()
                .eventId(eventId)
                .sequence(context.sequence)
                .orderId(order.getOrderId())
                .symbol(order.getSymbol())
                .timestamp(context.timestamp)
                .type(type)
                .tradeId(tradeId)
                .filledQuantity(order.getFilledQuantity())
                .remainingQuantity(order.getRemainingQuantity())
                .build();
    }

    /** 封装生成确定性事件编号所需的命令上下文。 */
    private record EventContext(long sequence, Instant timestamp, int offset) {
    }
}
