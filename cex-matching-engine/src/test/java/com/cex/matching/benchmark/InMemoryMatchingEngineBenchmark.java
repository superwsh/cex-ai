package com.cex.matching.benchmark;

import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.enums.OrderType;
import com.cex.matching.domain.enums.TimeInForce;
import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.model.OrderBook;
import com.cex.matching.domain.service.InMemoryMatchingEngine;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/** 单线程内存撮合基准入口，用于比较本地硬件上的吞吐和延迟。 */
public final class InMemoryMatchingEngineBenchmark {

    private static final String SYMBOL = "BTC_USDT";

    private InMemoryMatchingEngineBenchmark() {
    }

    /**
     * 执行基准测试。
     *
     * @param args 第一个参数为成交订单对数量，缺省为 10_000
     */
    public static void main(String[] args) {
        int pairCount = args.length == 0 ? 10_000 : Integer.parseInt(args[0]);
        BenchmarkResult result = run(pairCount);
        System.out.printf("订单命令数: %d%n吞吐量: %.0f commands/s%n平均延迟: %.0f ns%nP99 延迟: %d ns%n",
                result.commandCount(), result.commandsPerSecond(),
                result.averageLatencyNanos(), result.p99LatencyNanos());
    }

    /**
     * 在单线程中提交等量买单和卖单，以测量纯撮合路径。
     *
     * @param pairCount 需要生成的完全成交订单对数量，必须大于零
     * @return 该次执行的吞吐量和延迟指标
     */
    public static BenchmarkResult run(int pairCount) {
        if (pairCount <= 0) {
            throw new IllegalArgumentException("订单对数量必须大于零");
        }
        int commandCount = Math.multiplyExact(pairCount, 2);
        long[] latencyNanos = new long[commandCount];
        InMemoryMatchingEngine engine = createEngine();
        long startedAt = System.nanoTime();
        for (int index = 0; index < pairCount; index++) {
            long buyOrderId = index * 2L + 1L;
            recordLatency(engine, limitOrder(buyOrderId, OrderSide.BUY), latencyNanos, index * 2);
            recordLatency(engine, limitOrder(buyOrderId + 1L, OrderSide.SELL), latencyNanos, index * 2 + 1);
        }
        long elapsedNanos = System.nanoTime() - startedAt;
        return calculateResult(commandCount, elapsedNanos, latencyNanos);
    }

    private static InMemoryMatchingEngine createEngine() {
        AtomicLong tradeId = new AtomicLong();
        return new InMemoryMatchingEngine(new OrderBook(SYMBOL), tradeId::incrementAndGet);
    }

    private static MatchOrder limitOrder(long orderId, OrderSide side) {
        return MatchOrder.builder()
                .orderId(orderId).userId(1L).symbol(SYMBOL).side(side)
                .type(OrderType.LIMIT).price(new BigDecimal("100"))
                .quantity(BigDecimal.ONE).timeInForce(TimeInForce.GTC)
                .createdAt(Instant.EPOCH).sequence(orderId).build();
    }

    private static void recordLatency(InMemoryMatchingEngine engine, MatchOrder order,
                                      long[] latencyNanos, int index) {
        long startedAt = System.nanoTime();
        engine.process(order);
        latencyNanos[index] = System.nanoTime() - startedAt;
    }

    private static BenchmarkResult calculateResult(int commandCount, long elapsedNanos, long[] latencyNanos) {
        Arrays.sort(latencyNanos);
        long totalLatencyNanos = Arrays.stream(latencyNanos).sum();
        int p99Index = (int) Math.ceil(commandCount * 0.99D) - 1;
        double commandsPerSecond = commandCount * 1_000_000_000D / elapsedNanos;
        return new BenchmarkResult(commandCount, commandsPerSecond,
                totalLatencyNanos / (double) commandCount, latencyNanos[p99Index]);
    }

    /** 基准测试产生的聚合指标。 */
    public record BenchmarkResult(int commandCount, double commandsPerSecond,
                                  double averageLatencyNanos, long p99LatencyNanos) {
    }
}
