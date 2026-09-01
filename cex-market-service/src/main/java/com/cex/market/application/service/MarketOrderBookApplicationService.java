package com.cex.market.application.service;

import com.cex.common.kafka.event.market.OrderBookDeltaEvent;
import com.cex.common.kafka.event.market.PriceLevelChange;
import com.cex.market.domain.exception.MarketSequenceGapException;
import com.cex.market.domain.model.DeltaApplyResult;
import com.cex.market.domain.model.MarketOrderBook;
import com.cex.market.domain.model.MarketOrderBookSnapshot;
import com.cex.market.domain.model.MarketPriceLevel;
import com.cex.market.infrastructure.metrics.MarketMetrics;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/** 编排盘口增量应用、快照持久化、启动恢复和序号缺口重放。 */
@Service
@RequiredArgsConstructor
public class MarketOrderBookApplicationService {

    private final MarketOrderBookSnapshotRepository snapshotRepository;
    private final OrderBookEventReplayer eventReplayer;
    private final MarketBookTickerApplicationService bookTickerApplicationService;
    private final MarketMetrics marketMetrics;
    private final ConcurrentHashMap<String, MarketOrderBook> orderBooks = new ConcurrentHashMap<>();

    /**
     * 在 Kafka 消费启动前从数据库快照恢复全部本地订单簿。
     */
    @PostConstruct
    public void restoreSnapshots() {
        snapshotRepository.findAll().forEach(snapshot -> orderBooks.put(snapshot.symbol(), restore(snapshot)));
    }

    /**
     * 应用一条 Kafka 盘口增量；成功后先持久化快照，再刷新对外深度与最佳报价。
     *
     * @param event 盘口增量事件
     * @param partition Kafka 分区
     * @param offset Kafka 位点
     * @return 处理结果
     */
    public MarketOrderBookProcessingResult process(OrderBookDeltaEvent event, int partition, long offset) {
        validateKafkaPosition(event, partition, offset);
        AtomicReference<MarketOrderBookProcessingResult> resultReference = new AtomicReference<>();
        AtomicReference<MarketOrderBook> activeBookReference = new AtomicReference<>();
        orderBooks.compute(event.getSymbol(), (symbol, currentBook) -> {
            MarketOrderBook workingBook = currentBook == null ? loadOrCreate(symbol) : copyOf(currentBook);
            try {
                DeltaApplyResult applyResult = workingBook.applyDelta(event);
                if (applyResult == DeltaApplyResult.IGNORED_DUPLICATE) {
                    resultReference.set(new MarketOrderBookProcessingResult(applyResult, false, null));
                    return currentBook == null ? workingBook : currentBook;
                }
                MarketOrderBookSnapshot snapshot = workingBook.recoverySnapshot(partition, offset, event.getEventTime());
                snapshotRepository.save(snapshot);
                resultReference.set(new MarketOrderBookProcessingResult(applyResult, false, snapshot));
                activeBookReference.set(workingBook);
                return workingBook;
            } catch (MarketSequenceGapException exception) {
                marketMetrics.recordSequenceGap();
                try {
                    MarketOrderBook recoveredBook = recover(symbol, event, partition, offset);
                    MarketOrderBookSnapshot snapshot = recoveredBook.recoverySnapshot(partition, offset, event.getEventTime());
                    snapshotRepository.save(snapshot);
                    marketMetrics.recordOrderBookRecovery(true);
                    resultReference.set(new MarketOrderBookProcessingResult(DeltaApplyResult.APPLIED, true, snapshot));
                    activeBookReference.set(recoveredBook);
                    return recoveredBook;
                } catch (RuntimeException recoveryException) {
                    marketMetrics.recordOrderBookRecovery(false);
                    throw recoveryException;
                }
            }
        });
        MarketOrderBookProcessingResult result = resultReference.get();
        if (result.applyResult() == DeltaApplyResult.APPLIED) {
            bookTickerApplicationService.refresh(activeBookReference.get(), event.getEventTime());
        }
        return result;
    }

    /**
     * 从已保存的快照重建运行时订单簿，缺失快照时仅允许从 sequence=0 开始。
     *
     * @param symbol 交易对
     * @return 可接收盘口增量的订单簿
     */
    private MarketOrderBook loadOrCreate(String symbol) {
        MarketOrderBookSnapshot snapshot = snapshotRepository.findBySymbol(symbol);
        if (snapshot != null) {
            return restore(snapshot);
        }
        MarketOrderBook orderBook = new MarketOrderBook(symbol);
        orderBook.markRecovering();
        orderBook.loadSnapshot(0L, List.of(), List.of());
        return orderBook;
    }

    /**
     * 使用上一个持久化快照重放 Kafka WAL，直到包含当前检测到缺口的记录。
     *
     * @param symbol 交易对
     * @param targetEvent 当前无法连续应用的事件
     * @param partition Kafka 分区
     * @param offset 当前 Kafka 位点
     * @return 已恢复到当前记录的订单簿
     */
    private MarketOrderBook recover(String symbol, OrderBookDeltaEvent targetEvent, int partition, long offset) {
        MarketOrderBookSnapshot snapshot = snapshotRepository.findBySymbol(symbol);
        if (snapshot == null) {
            throw new IllegalStateException("订单簿缺少恢复快照，无法重放: symbol=" + symbol);
        }
        MarketOrderBook recoveredBook = restore(snapshot);
        List<OrderBookDeltaEvent> events = eventReplayer.replayTo(snapshot, partition, offset);
        for (OrderBookDeltaEvent event : events) {
            recoveredBook.applyDelta(event);
        }
        if (recoveredBook.getSequence() != targetEvent.getSequence()) {
            throw new IllegalStateException("Kafka 重放未恢复到目标序号: symbol=" + symbol + ", expected="
                    + targetEvent.getSequence() + ", actual=" + recoveredBook.getSequence());
        }
        return recoveredBook;
    }

    /**
     * 从持久化快照创建活动订单簿。
     *
     * @param snapshot 持久化快照
     * @return 已处于 ACTIVE 状态的订单簿
     */
    private MarketOrderBook restore(MarketOrderBookSnapshot snapshot) {
        MarketOrderBook orderBook = new MarketOrderBook(snapshot.symbol());
        orderBook.markRecovering();
        orderBook.loadSnapshot(snapshot.sequence(), toChanges(snapshot.bids()), toChanges(snapshot.asks()));
        return orderBook;
    }

    /**
     * 深复制当前订单簿，确保快照写入失败不会污染已确认的内存状态。
     *
     * @param currentBook 当前已确认订单簿
     * @return 用于本次事件处理的工作副本
     */
    private MarketOrderBook copyOf(MarketOrderBook currentBook) {
        var snapshot = currentBook.depthSnapshot(Integer.MAX_VALUE, System.currentTimeMillis());
        MarketOrderBook copy = new MarketOrderBook(currentBook.getSymbol());
        copy.loadSnapshot(currentBook.getSequence(), toChanges(snapshot.bids()), toChanges(snapshot.asks()));
        return copy;
    }

    /**
     * 将只读价格档位转换为订单簿加载需要的变更档位。
     *
     * @param levels 价格档位
     * @return 价格档位变更
     */
    private List<PriceLevelChange> toChanges(List<MarketPriceLevel> levels) {
        return levels.stream().map(level -> PriceLevelChange.builder().price(level.price()).quantity(level.quantity()).build())
                .toList();
    }

    /**
     * 校验 Kafka 记录的位置及事件基本信息。
     *
     * @param event 盘口增量
     * @param partition Kafka 分区
     * @param offset Kafka 位点
     */
    private void validateKafkaPosition(OrderBookDeltaEvent event, int partition, long offset) {
        if (event == null || event.getSymbol() == null || event.getSymbol().isBlank() || partition < 0 || offset < 0) {
            throw new IllegalArgumentException("盘口 Kafka 记录字段非法");
        }
    }
}
