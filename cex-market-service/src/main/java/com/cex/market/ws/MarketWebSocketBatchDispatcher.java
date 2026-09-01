package com.cex.market.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/** 对行情 WebSocket 消息执行短周期批量、合并、单次序列化和扇出。 */
@Component
@RequiredArgsConstructor
public class MarketWebSocketBatchDispatcher {

    private final ObjectMapper objectMapper;
    private final MarketWebSocketSessionManager sessionManager;
    private final ConcurrentLinkedQueue<PendingMessage> pendingTrades = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<String, PendingMessage> conflatedMessages = new ConcurrentHashMap<>();
    private final AtomicInteger pendingTradeCount = new AtomicInteger();
    private final AtomicBoolean flushing = new AtomicBoolean();

    @Value("${netty.ws.broadcast.trade-batch-size:100}")
    private int tradeBatchSize = 100;

    @Value("${netty.ws.broadcast.max-pending-trades:10000}")
    private int maxPendingTrades = 10_000;

    /** 校验广播队列配置。 */
    @PostConstruct
    public void validateConfiguration() {
        if (tradeBatchSize <= 0 || maxPendingTrades <= 0) {
            throw new IllegalStateException("WebSocket 广播批量配置非法");
        }
    }

    /**
     * 入队一笔不能合并丢失的逐笔成交。
     *
     * @param type 推送类型
     * @param channelKey 规范频道名称
     * @param data 成交数据
     */
    public void enqueueTrade(String type, String channelKey, Object data) {
        PendingMessage message = new PendingMessage(type, channelKey, data, System.currentTimeMillis());
        while (!reserveTradeQueueSlot()) {
            LockSupport.parkNanos(1_000_000L);
        }
        pendingTrades.offer(message);
    }

    /**
     * 写入可以按频道合并的最新行情快照。
     *
     * @param type 推送类型
     * @param channelKey 规范频道名称
     * @param data 最新行情数据
     */
    public void conflate(String type, String channelKey, Object data) {
        conflatedMessages.put(channelKey, new PendingMessage(type, channelKey, data, System.currentTimeMillis()));
    }

    /** 每个短周期批量刷新待发送行情，避免写入生产线程逐条扇出。 */
    @Scheduled(fixedDelayString = "${netty.ws.broadcast.flush-interval-ms:50}")
    public void flush() {
        if (!flushing.compareAndSet(false, true)) {
            return;
        }
        try {
            flushTradeBatch();
            flushConflatedMessages();
        } finally {
            flushing.set(false);
        }
    }

    /**
     * 将一批成交按频道聚合，确保同交易对在批次内保持队列顺序。
     */
    private void flushTradeBatch() {
        Map<String, List<Object>> tradesByChannel = new LinkedHashMap<>();
        for (int index = 0; index < tradeBatchSize; index++) {
            PendingMessage message = pendingTrades.poll();
            if (message == null) {
                break;
            }
            pendingTradeCount.decrementAndGet();
            tradesByChannel.computeIfAbsent(message.channelKey(), ignored -> new ArrayList<>()).add(message.data());
        }
        tradesByChannel.forEach((channelKey, trades) -> dispatch(new PendingMessage("TRADE_BATCH", channelKey,
                List.copyOf(trades), System.currentTimeMillis())));
    }

    /**
     * 发送当前刷新周期内每个频道最后一份合并行情。
     */
    private void flushConflatedMessages() {
        conflatedMessages.forEach((channelKey, message) -> {
            if (conflatedMessages.remove(channelKey, message)) {
                dispatch(message);
            }
        });
    }

    /**
     * 将一条广播消息序列化一次，并把引用副本扇出给对应频道订阅者。
     *
     * @param message 待发送消息
     */
    private void dispatch(PendingMessage message) {
        try {
            String serialized = objectMapper.writeValueAsString(new PushMessage(message.type(), message.channelKey(),
                    message.data(), message.serverTime()));
            sessionManager.publish(message.channelKey(), new TextWebSocketFrame(serialized));
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("WebSocket 行情消息序列化失败: channel=" + message.channelKey(), exception);
        }
    }

    /**
     * 原子预留一条逐笔成交队列容量；队列满时返回 false 以对生产线程施加背压。
     *
     * @return 是否已成功预留容量
     */
    private boolean reserveTradeQueueSlot() {
        while (true) {
            int current = pendingTradeCount.get();
            if (current >= maxPendingTrades) {
                return false;
            }
            if (pendingTradeCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /** 内部待发送行情记录。 */
    private record PendingMessage(String type, String channelKey, Object data, long serverTime) {
    }

    /** WebSocket 对外推送消息结构。 */
    private record PushMessage(String type, String channel, Object data, long serverTime) {
    }
}
