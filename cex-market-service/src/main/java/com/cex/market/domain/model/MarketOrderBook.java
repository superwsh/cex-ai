package com.cex.market.domain.model;

import com.cex.common.kafka.event.market.OrderBookDeltaEvent;
import com.cex.common.kafka.event.market.PriceLevelChange;
import com.cex.market.domain.exception.MarketSequenceGapException;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 由行情事件重建的单交易对 Level 2 聚合订单簿。
 * 此模型由同一个 symbol 的单一消费执行器独占修改，不提供并发写入保证。
 */
public final class MarketOrderBook {

    private final String symbol;
    private final NavigableMap<BigDecimal, BigDecimal> bids = new TreeMap<>(Comparator.reverseOrder());
    private final NavigableMap<BigDecimal, BigDecimal> asks = new TreeMap<>();
    private long sequence;
    private MarketDataStatus status = MarketDataStatus.INIT;

    /**
     * 创建未加载快照的盘口。
     *
     * @param symbol 交易对
     */
    public MarketOrderBook(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        this.symbol = symbol;
    }

    /**
     * 用已校验的快照原子替换本地盘口，并恢复为可用状态。
     *
     * @param snapshotSequence 快照最后包含的盘口序号
     * @param snapshotBids 快照买方档位
     * @param snapshotAsks 快照卖方档位
     */
    public void loadSnapshot(long snapshotSequence, List<PriceLevelChange> snapshotBids,
                             List<PriceLevelChange> snapshotAsks) {
        if (snapshotSequence < 0) {
            throw new IllegalArgumentException("快照序号不能为负数");
        }
        NavigableMap<BigDecimal, BigDecimal> newBids = new TreeMap<>(Comparator.reverseOrder());
        NavigableMap<BigDecimal, BigDecimal> newAsks = new TreeMap<>();
        applyChanges(newBids, snapshotBids, "快照买方档位");
        applyChanges(newAsks, snapshotAsks, "快照卖方档位");
        bids.clear();
        bids.putAll(newBids);
        asks.clear();
        asks.putAll(newAsks);
        sequence = snapshotSequence;
        status = MarketDataStatus.ACTIVE;
    }

    /**
     * 在序号连续时应用盘口增量。Kafka 重复投递的旧序号会被安全忽略。
     *
     * @param event 同一交易对按 Kafka 分区顺序到达的盘口增量
     * @return 本次事件是已应用还是重复事件
     */
    public DeltaApplyResult applyDelta(OrderBookDeltaEvent event) {
        validateEvent(event);
        if (status != MarketDataStatus.ACTIVE) {
            throw new IllegalStateException("行情盘口未处于可用状态: symbol=" + symbol + ", status=" + status);
        }
        if (event.getSequence() <= sequence) {
            return DeltaApplyResult.IGNORED_DUPLICATE;
        }
        if (event.getPreviousSequence() != sequence) {
            status = MarketDataStatus.INVALID;
            throw new MarketSequenceGapException(symbol, sequence, event.getPreviousSequence(), event.getSequence());
        }
        applyChanges(bids, event.getBids(), "买方档位");
        applyChanges(asks, event.getAsks(), "卖方档位");
        sequence = event.getSequence();
        return DeltaApplyResult.APPLIED;
    }

    /**
     * 将盘口转为恢复中的状态，供恢复协调器在获取快照前调用。
     */
    public void markRecovering() {
        if (status != MarketDataStatus.INVALID && status != MarketDataStatus.INIT) {
            throw new IllegalStateException("仅 INVALID 或 INIT 状态可以进入恢复: status=" + status);
        }
        status = MarketDataStatus.RECOVERING;
    }

    /**
     * 获取指定深度的买方档位，价格从高到低。
     *
     * @param limit 最大返回档位数
     * @return 不可修改的价格档位列表
     */
    public List<MarketPriceLevel> bidDepth(int limit) {
        return depth(bids, limit);
    }

    /**
     * 获取指定深度的卖方档位，价格从低到高。
     *
     * @param limit 最大返回档位数
     * @return 不可修改的价格档位列表
     */
    public List<MarketPriceLevel> askDepth(int limit) {
        return depth(asks, limit);
    }

    /**
     * 获取当前最佳买方档位。
     *
     * @return 价格最高的买方档位；盘口为空时为空
     */
    public Optional<MarketPriceLevel> bestBid() {
        return bestLevel(bids);
    }

    /**
     * 获取当前最佳卖方档位。
     *
     * @return 价格最低的卖方档位；盘口为空时为空
     */
    public Optional<MarketPriceLevel> bestAsk() {
        return bestLevel(asks);
    }

    /**
     * 在当前已应用序号上创建不可变盘口快照。
     *
     * @param limit 买卖两侧最大档位数
     * @param timestamp 快照创建时间（毫秒时间戳）
     * @return 可供读取层使用的深度快照
     */
    public MarketDepthSnapshot depthSnapshot(int limit, long timestamp) {
        if (timestamp <= 0) {
            throw new IllegalArgumentException("快照时间必须大于零");
        }
        return new MarketDepthSnapshot(symbol, sequence, bidDepth(limit), askDepth(limit), timestamp);
    }

    /**
     * 生成包含 Kafka 位点的完整恢复快照。
     *
     * @param kafkaPartition 处理该交易对增量的 Kafka 分区
     * @param kafkaOffset 已应用事件的 Kafka 位点
     * @param timestamp 快照创建时间（毫秒时间戳）
     * @return 可持久化的完整订单簿快照
     */
    public MarketOrderBookSnapshot recoverySnapshot(int kafkaPartition, long kafkaOffset, long timestamp) {
        if (kafkaPartition < 0 || kafkaOffset < 0 || timestamp <= 0) {
            throw new IllegalArgumentException("订单簿恢复快照参数非法");
        }
        return new MarketOrderBookSnapshot(symbol, sequence, bidDepth(Integer.MAX_VALUE), askDepth(Integer.MAX_VALUE),
                kafkaPartition, kafkaOffset, timestamp);
    }

    /**
     * 获取订单簿所属交易对。
     *
     * @return 交易对
     */
    public String getSymbol() {
        return symbol;
    }

    /**
     * 获取当前已应用的最后盘口序号。
     *
     * @return 最后盘口序号
     */
    public long getSequence() {
        return sequence;
    }

    /**
     * 获取当前行情数据状态。
     *
     * @return 行情状态
     */
    public MarketDataStatus getStatus() {
        return status;
    }

    /**
     * 应用一组“价格 -> 聚合数量”变化。
     *
     * @param levels 待修改的价格档位
     * @param changes 新档位数量
     * @param fieldName 用于异常说明的字段名称
     */
    private void applyChanges(NavigableMap<BigDecimal, BigDecimal> levels, List<PriceLevelChange> changes,
                              String fieldName) {
        if (changes == null) {
            throw new IllegalArgumentException(fieldName + "不能为空");
        }
        for (PriceLevelChange change : changes) {
            if (change == null || change.getPrice() == null || change.getPrice().signum() <= 0
                    || change.getQuantity() == null || change.getQuantity().signum() < 0) {
                throw new IllegalArgumentException(fieldName + "包含非法价格档位");
            }
            if (change.getQuantity().signum() == 0) {
                levels.remove(change.getPrice());
            } else {
                levels.put(change.getPrice(), change.getQuantity());
            }
        }
    }

    /**
     * 校验增量事件是否属于本订单簿且拥有有效的协议字段。
     *
     * @param event 待应用的盘口增量
     */
    private void validateEvent(OrderBookDeltaEvent event) {
        Objects.requireNonNull(event, "盘口增量事件不能为空");
        if (!symbol.equals(event.getSymbol()) || event.getEventId() == null || event.getEventId().isBlank()
                || event.getEventVersion() == null || event.getEventVersion() != 1
                || event.getSequence() < 0 || event.getPreviousSequence() < 0
                || (event.getSourceSequence() != null && event.getSourceSequence() < 0)
                || event.getEventTime() <= 0) {
            throw new IllegalArgumentException("盘口增量事件字段非法");
        }
    }

    /**
     * 截取有序盘口档位。
     *
     * @param levels 有序价格档位
     * @param limit 最大返回档位数
     * @return 不可修改的盘口深度
     */
    private List<MarketPriceLevel> depth(NavigableMap<BigDecimal, BigDecimal> levels, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("深度限制必须大于零");
        }
        List<MarketPriceLevel> depth = new ArrayList<>(Math.min(limit, levels.size()));
        for (var entry : levels.entrySet()) {
            if (depth.size() == limit) {
                break;
            }
            depth.add(new MarketPriceLevel(entry.getKey(), entry.getValue()));
        }
        return List.copyOf(depth);
    }

    /**
     * 从有序档位中读取最优一档并复制为只读输出对象。
     *
     * @param levels 有序价格档位
     * @return 最优档位；不存在时为空
     */
    private Optional<MarketPriceLevel> bestLevel(NavigableMap<BigDecimal, BigDecimal> levels) {
        if (levels.isEmpty()) {
            return Optional.empty();
        }
        var entry = levels.firstEntry();
        return Optional.of(new MarketPriceLevel(entry.getKey(), entry.getValue()));
    }
}
