package com.cex.clearing.application.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/** 清算结算核心业务指标。Micrometer 点号名称会映射为 Prometheus 下划线名称。 */
@Component
public class ClearingMetrics {

    private final Counter settlementSuccess;
    private final Counter settlementFailed;
    private final Counter settlementRetry;
    private final Counter settlementManualReview;
    private final Counter settlementDuplicate;
    private final Counter settlementInsufficientBalance;
    private final Counter settlementDbDeadlock;
    private final Counter reconciliationDifference;
    private final Counter alerts;
    private final Timer settlementLatency;
    private final AtomicLong outboxPending = new AtomicLong();
    private final AtomicLong outboxFailed = new AtomicLong();
    private final AtomicLong settlementConsumerLag = new AtomicLong();

    /** 注册清算指标及 Outbox 状态仪表。 */
    public ClearingMetrics(MeterRegistry registry) {
        settlementSuccess = Counter.builder("settlement.success").register(registry);
        settlementFailed = Counter.builder("settlement.failed").register(registry);
        settlementRetry = Counter.builder("settlement.retry").register(registry);
        settlementManualReview = Counter.builder("settlement.manual.review").register(registry);
        settlementDuplicate = Counter.builder("settlement.duplicate.trade").register(registry);
        settlementInsufficientBalance = Counter.builder("settlement.insufficient.balance").register(registry);
        settlementDbDeadlock = Counter.builder("settlement.db.deadlock").register(registry);
        reconciliationDifference = Counter.builder("reconciliation.difference").register(registry);
        alerts = Counter.builder("clearing.alert").register(registry);
        settlementLatency = Timer.builder("settlement.latency").publishPercentileHistogram().register(registry);
        Gauge.builder("outbox.pending.total", outboxPending, AtomicLong::get).register(registry);
        Gauge.builder("outbox.failed.total", outboxFailed, AtomicLong::get).register(registry);
        Gauge.builder("settlement.consumer.lag", settlementConsumerLag, AtomicLong::get).register(registry);
    }

    /** 记录成功结算及端到端处理时延。 */
    public void recordSettlementSuccess(long startedNanos) {
        settlementSuccess.increment();
        settlementLatency.record(Duration.ofNanos(System.nanoTime() - startedNanos));
    }

    /** 记录结算失败。 */
    public void recordSettlementFailure(String errorCode) {
        settlementFailed.increment();
        if ("INSUFFICIENT_SETTLEMENT_BALANCE".equals(errorCode)) {
            settlementInsufficientBalance.increment();
        }
        if (errorCode != null && errorCode.contains("DEADLOCK")) {
            settlementDbDeadlock.increment();
        }
    }

    /** 记录一次持久化或 Kafka 重试。 */
    public void recordSettlementRetry() {
        settlementRetry.increment();
    }

    /** 记录进入人工复核。 */
    public void recordManualReview() {
        settlementManualReview.increment();
    }

    /** 记录重复成交消息。 */
    public void recordDuplicateTrade() {
        settlementDuplicate.increment();
    }

    /** 记录本轮对账差异数量。 */
    public void recordReconciliationDifferences(int count) {
        reconciliationDifference.increment(count);
    }

    /** 更新当前扫描到的 Outbox 待发送数量。 */
    public void setOutboxPending(long count) {
        outboxPending.set(count);
    }

    /** 更新当前 FAILED Outbox 数量。 */
    public void setOutboxFailed(long count) {
        outboxFailed.set(count);
    }

    /** 更新成交事件时间戳到当前消费时刻的延迟毫秒数。 */
    public void recordConsumerLag(Long eventTimestamp) {
        if (eventTimestamp != null) {
            settlementConsumerLag.set(Math.max(0, System.currentTimeMillis() - eventTimestamp));
        }
    }

    /** 记录一次结构化资金告警。 */
    public void recordAlert() {
        alerts.increment();
    }
}
