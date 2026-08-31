package com.cex.matching.application.command;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.matching.domain.model.MatchResult;
import com.cex.matching.domain.model.OrderBookSnapshot;
import com.cex.matching.domain.model.ProcessedMatchResultSnapshot;
import com.cex.matching.domain.sequence.SequenceGapException;
import com.cex.matching.application.mapper.OrderEventMapper;
import com.cex.matching.application.observability.MatchingMetrics;
import com.cex.matching.application.port.outbound.MatchingCommandJournal;
import com.cex.matching.application.recovery.model.RecordedMatchingCommand;
import com.cex.matching.application.recovery.support.InMemoryMatchingCommandJournal;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** 按交易对创建撮合实例，并在进程内去重 Kafka 重投的订单事件。 */
@Component
public class MatchingEngineRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(MatchingEngineRegistry.class);

    private final OrderEventMapper orderEventMapper = new OrderEventMapper();
    private final MatchingCommandJournal commandJournal;
    private final MatchingMetrics matchingMetrics;
    private final AtomicLong tradeIdGenerator = new AtomicLong();
    private final ConcurrentHashMap<String, SymbolMatchingEngine> engines = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MatchResult> processedResults = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ProcessedMatchResultSnapshot>>
            resultsPendingSnapshot = new ConcurrentHashMap<>();

    /** 创建仅供单元测试使用的内存日志注册表。 */
    public MatchingEngineRegistry() {
        this(new InMemoryMatchingCommandJournal(), MatchingMetrics.noop());
    }

    /**
     * 创建使用指定预写日志的撮合注册表。
     *
     * @param commandJournal 命令在撮合前必须写入的持久化日志
     */
    public MatchingEngineRegistry(MatchingCommandJournal commandJournal) {
        this(commandJournal, MatchingMetrics.noop());
    }

    /**
     * 创建使用指定预写日志和监控组件的撮合注册表。
     *
     * @param commandJournal 命令在撮合前必须写入的持久化日志
     * @param matchingMetrics 撮合命令指标记录器
     */
    @Autowired
    public MatchingEngineRegistry(MatchingCommandJournal commandJournal, MatchingMetrics matchingMetrics) {
        this.commandJournal = Objects.requireNonNull(commandJournal, "撮合命令日志不能为空");
        this.matchingMetrics = Objects.requireNonNull(matchingMetrics, "撮合指标记录器不能为空");
    }

    /**
     * 幂等地处理订单事件。
     * 同交易对事件必须由 Kafka 的 symbol 键投递到同一分区并串行消费；本类并不为订单簿加锁。
     *
     * @param event Kafka 收到的订单事件
     * @return 首次处理或 Kafka 重试时均返回同一撮合结果
     */
    public Optional<MatchResult> process(OrderEvent event) {
        validateEvent(event);
        MatchResult previousResult = processedResults.get(event.getEventId());
        if (previousResult != null) {
            return Optional.of(previousResult);
        }
        try {
            SymbolMatchingEngine engine = engines.computeIfAbsent(event.getSymbol(), this::createEngine);
            if (event.getSequence() != null && event.getSequence() < engine.nextSequence()) {
                LOGGER.warn("忽略已完成的历史撮合命令: eventId={}, symbol={}, sequence={}, lastSequence={}",
                        event.getEventId(), event.getSymbol(), event.getSequence(), engine.nextSequence() - 1);
                return Optional.empty();
            }
            long sequence = resolveSequence(event, engine);
            long commandStart = System.nanoTime();
            commandJournal.append(event.getSymbol(), sequence, event);
            long walDuration = System.nanoTime() - commandStart;
            MatchResult result = engine.processAtSequence(event, sequence);
            rememberProcessedResult(event, result);
            matchingMetrics.recordCommand(event, result, Duration.ofNanos(System.nanoTime() - commandStart),
                    Duration.ofNanos(walDuration), engine.activeOrderCount());
            logCommand(event, sequence, result);
            return Optional.of(result);
        } catch (RuntimeException exception) {
            throw exception;
        }
    }

    /**
     * 上游序号必须与当前交易对期望值完全一致；旧版空序号仅作兼容回退。
     */
    private long resolveSequence(OrderEvent event, SymbolMatchingEngine engine) {
        long expected = engine.nextSequence();
        Long supplied = event.getSequence();
        if (supplied == null) {
            return expected;
        }
        if (supplied > expected) {
            matchingMetrics.recordSequenceGap(event.getSymbol());
            throw new SequenceGapException(event.getSymbol(), expected, supplied);
        }
        return supplied;
    }

    /**
     * 获取指定交易对在当前命令边界的订单簿快照。
     *
     * @param symbol 需要快照的交易对
     * @return 对应交易对尚未创建撮合实例时为空
     */
    public Optional<OrderBookSnapshot> snapshot(String symbol) {
        SymbolMatchingEngine engine = engines.get(symbol);
        return engine == null ? Optional.empty() : Optional.of(withRecoveryData(symbol, engine.snapshot()));
    }

    /** 返回当前进程中已建立撮合状态的全部交易对。 */
    public java.util.Set<String> activeSymbols() {
        return java.util.Set.copyOf(engines.keySet());
    }

    /**
     * 从快照替换指定交易对的热状态；仅允许在启动恢复阶段调用。
     *
     * @param snapshot 已读取并校验的订单簿快照
     */
    public void restore(OrderBookSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "订单簿快照不能为空");
        if (engines.containsKey(snapshot.symbol())) {
            throw new IllegalStateException("撮合实例已存在，禁止覆盖恢复状态: " + snapshot.symbol());
        }
        engines.put(snapshot.symbol(), SymbolMatchingEngine.restore(snapshot, orderEventMapper,
                tradeIdGenerator::incrementAndGet));
        tradeIdGenerator.accumulateAndGet(snapshot.tradeIdHighWaterMark(), Math::max);
        for (ProcessedMatchResultSnapshot result : snapshot.processedResults()) {
            processedResults.put(result.eventId(), result.toMatchResult());
        }
    }

    /**
     * 重放一个交易对快照序号后的命令，并验证每条命令连续。
     *
     * @param symbol 需要重放的交易对
     * @param snapshotSequence 快照最后命令序号；无快照时传零
     */
    public void replay(String symbol, long snapshotSequence) {
        SymbolMatchingEngine engine = engines.computeIfAbsent(symbol, this::createEngine);
        for (RecordedMatchingCommand command : commandJournal.readAfter(symbol, snapshotSequence)) {
            MatchResult result = engine.processAtSequence(command.event(), command.sequence());
            rememberProcessedResult(command.event(), result);
        }
    }

    /**
     * 在快照成功写入后清理已被该快照持久化的幂等结果。
     *
     * @param symbol 已完成快照保存的交易对
     */
    public void confirmSnapshotPersisted(String symbol) {
        ConcurrentHashMap<String, ProcessedMatchResultSnapshot> persisted = resultsPendingSnapshot.remove(symbol);
        if (persisted != null) {
            persisted.keySet().forEach(processedResults::remove);
        }
    }

    /**
     * 创建一个仅归属指定交易对的撮合实例。
     *
     * @param symbol Kafka 分区键中的交易对
     * @return 新建的单交易对撮合实例
     */
    private SymbolMatchingEngine createEngine(String symbol) {
        return new SymbolMatchingEngine(symbol, orderEventMapper, tradeIdGenerator::incrementAndGet);
    }

    /**
     * 将全局成交编号生成器的当前高水位写入交易对快照。
     *
     * @param snapshot 订单簿生成的结构快照
     * @return 带全局成交编号高水位的完整快照
     */
    private OrderBookSnapshot withRecoveryData(String symbol, OrderBookSnapshot snapshot) {
        List<ProcessedMatchResultSnapshot> results = resultsPendingSnapshot
                .getOrDefault(symbol, new ConcurrentHashMap<>()).values().stream().toList();
        return new OrderBookSnapshot(snapshot.symbol(), snapshot.sequence(), tradeIdGenerator.get(), snapshot.orders(), results);
    }

    /**
     * 缓存命令结果，并保留到下一次成功快照以支撑 Kafka 重投。
     *
     * @param event 已成功处理的订单事件
     * @param result 对应的撮合结果
     */
    private void rememberProcessedResult(OrderEvent event, MatchResult result) {
        processedResults.put(event.getEventId(), result);
        resultsPendingSnapshot.computeIfAbsent(event.getSymbol(), key -> new ConcurrentHashMap<>())
                .put(event.getEventId(), ProcessedMatchResultSnapshot.from(event.getEventId(), result));
    }

    /**
     * 记录命令与逐笔成交的可追溯日志，不包含用户敏感信息。
     *
     * @param event 已处理订单事件
     * @param sequence 本次命令序号
     * @param result 对应的撮合结果
     */
    private void logCommand(OrderEvent event, long sequence, MatchResult result) {
        LOGGER.info("撮合命令完成: eventId={}, sequence={}, orderId={}, symbol={}, action={}, side={}, price={}, quantity={}, status={}, remainingQuantity={}",
                event.getEventId(), sequence, event.getOrderId(), event.getSymbol(), event.getAction(), event.getSide(),
                event.getPrice(), event.getQuantity(), result.getFinalStatus(), result.getRemainingQuantity());
        result.getTrades().forEach(trade -> LOGGER.info(
                "撮合成交生成: sequence={}, tradeId={}, symbol={}, makerOrderId={}, takerOrderId={}, price={}, quantity={}",
                trade.getSequence(), trade.getTradeId(), trade.getSymbol(), trade.getMakerOrderId(),
                trade.getTakerOrderId(), trade.getPrice(), trade.getQuantity()));
    }

    /**
     * 校验用于跨服务幂等和分区路由的必填字段。
     *
     * @param event 待处理订单事件
     */
    private void validateEvent(OrderEvent event) {
        Objects.requireNonNull(event, "订单事件不能为空");
        if (event.getEventId() == null || event.getEventId().isBlank()) {
            throw new IllegalArgumentException("订单事件编号不能为空");
        }
        if (event.getSymbol() == null || event.getSymbol().isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
    }
}
