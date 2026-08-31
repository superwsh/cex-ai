package com.cex.matching.application.observability;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.matching.domain.enums.OrderStatus;
import com.cex.matching.domain.model.MatchResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** 记录撮合命令边界的业务指标，且不在领域订单簿中引入监控依赖。 */
@Component
public class MatchingMetrics {

    private final MeterRegistry meterRegistry;
    private final ConcurrentHashMap<String, AtomicLong> activeOrderCounts = new ConcurrentHashMap<>();

    /**
     * 创建向 Micrometer 注册指标的监控组件。
     *
     * @param meterRegistry 指标注册表
     */
    public MatchingMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "指标注册表不能为空");
    }

    /** 创建仅供不启动 Spring 容器的单元测试使用的空监控组件。 */
    private MatchingMetrics() {
        this.meterRegistry = null;
    }

    /**
     * 获取不产生指标写入的监控组件。
     *
     * @return 空监控组件
     */
    public static MatchingMetrics noop() {
        return new MatchingMetrics();
    }

    /**
     * 记录一次已执行命令的时延、结果和订单簿规模。
     *
     * @param event 已处理订单事件
     * @param result 撮合结果
     * @param commandDuration 从 WAL 写入到撮合结束的耗时
     * @param walDuration WAL 强制刷盘耗时
     * @param activeOrderCount 命令结束后的活动订单数
     */
    public void recordCommand(OrderEvent event, MatchResult result, Duration commandDuration,
                              Duration walDuration, long activeOrderCount) {
        if (meterRegistry == null) {
            return;
        }
        String symbol = event.getSymbol();
        String action = event.getAction().name();
        Counter.builder("cex.matching.commands.total").tag("symbol", symbol).tag("action", action)
                .tag("status", result.getFinalStatus().name()).register(meterRegistry).increment();
        Timer.builder("cex.matching.command.latency").tag("symbol", symbol).publishPercentileHistogram()
                .register(meterRegistry).record(commandDuration);
        Timer.builder("cex.matching.wal.write.latency").tag("symbol", symbol).publishPercentileHistogram()
                .register(meterRegistry).record(walDuration);
        Counter.builder("cex.matching.trades.total").tag("symbol", symbol).register(meterRegistry)
                .increment(result.getTrades().size());
        if (result.getFinalStatus() == OrderStatus.REJECTED) {
            Counter.builder("cex.matching.rejects.total").tag("symbol", symbol).tag("action", action)
                    .register(meterRegistry).increment();
        }
        activeOrderCounts.computeIfAbsent(symbol, this::registerActiveOrderGauge).set(activeOrderCount);
    }

    /**
     * 记录一次成功快照的完整落盘耗时。
     *
     * @param symbol 快照所属交易对
     * @param duration 创建、写入与裁剪 WAL 的总耗时
     */
    public void recordSnapshot(String symbol, Duration duration) {
        if (meterRegistry == null) {
            return;
        }
        Timer.builder("cex.matching.snapshot.latency").tag("symbol", symbol).publishPercentileHistogram()
                .register(meterRegistry).record(duration);
    }

    /**
     * 记录一次启动恢复中读取快照并重放 WAL 的耗时。
     *
     * @param symbol 恢复的交易对
     * @param duration 快照恢复与 WAL 重放总耗时
     */
    public void recordReplay(String symbol, Duration duration) {
        if (meterRegistry == null) {
            return;
        }
        Timer.builder("cex.matching.replay.latency").tag("symbol", symbol).publishPercentileHistogram()
                .register(meterRegistry).record(duration);
    }

    /** 记录一次需要暂停消费并恢复的命令序号缺口。 */
    public void recordSequenceGap(String symbol) {
        if (meterRegistry != null) {
            Counter.builder("cex.matching.sequence.gap.total").tag("symbol", symbol)
                    .register(meterRegistry).increment();
        }
    }

    /**
     * 为首次出现的交易对注册活动订单数仪表。
     *
     * @param symbol 交易对
     * @return 承载当前订单数的可变值
     */
    private AtomicLong registerActiveOrderGauge(String symbol) {
        AtomicLong count = new AtomicLong();
        Gauge.builder("cex.matching.active.orders", count, AtomicLong::get).tag("symbol", symbol)
                .register(meterRegistry);
        return count;
    }
}
