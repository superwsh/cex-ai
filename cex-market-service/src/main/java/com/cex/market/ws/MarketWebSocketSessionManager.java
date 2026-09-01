package com.cex.market.ws;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.GlobalEventExecutor;
import com.cex.market.infrastructure.metrics.MarketMetrics;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** 管理 WebSocket 连接及其内存订阅关系。 */
@Component
@RequiredArgsConstructor
public class MarketWebSocketSessionManager {

    private static final int MAX_SUBSCRIPTIONS_PER_CONNECTION = 100;
    private final MarketMetrics marketMetrics;
    private final ConcurrentHashMap<ChannelId, Set<String>> subscriptions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ChannelGroup> channelSubscribers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChannelId, Integer> consecutiveUnwritableCounts = new ConcurrentHashMap<>();
    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    @Value("${netty.ws.broadcast.max-consecutive-unwritable:3}")
    private int maxConsecutiveUnwritable = 3;

    /** 校验慢客户端保护阈值。 */
    @PostConstruct
    public void validateConfiguration() {
        if (maxConsecutiveUnwritable <= 0) {
            throw new IllegalStateException("WebSocket 慢客户端保护阈值必须大于零");
        }
    }

    /**
     * 在 WebSocket 握手完成后登记连接。
     *
     * @param channel 已完成握手的连接
     */
    public void register(Channel channel) {
        channels.add(channel);
        subscriptions.putIfAbsent(channel.id(), ConcurrentHashMap.newKeySet());
        marketMetrics.updateWebSocketConnections(channels.size());
    }

    /**
     * 在连接断开时释放其全部订阅。
     *
     * @param channel 已断开的连接
     */
    public void unregister(Channel channel) {
        Set<String> sessionSubscriptions = subscriptions.remove(channel.id());
        if (sessionSubscriptions != null) {
            sessionSubscriptions.forEach(channelKey -> removeFromChannelIndex(channelKey, channel));
        }
        consecutiveUnwritableCounts.remove(channel.id());
        channels.remove(channel);
        marketMetrics.updateWebSocketConnections(channels.size());
    }

    /**
     * 为连接添加频道订阅。
     *
     * @param channel 连接
     * @param channelKeys 规范频道名称
     * @return 本次实际新增的订阅
     */
    public Set<String> subscribe(Channel channel, Collection<String> channelKeys) {
        Set<String> sessionSubscriptions = requireSession(channel);
        Set<String> newSubscriptions = new LinkedHashSet<>(channelKeys);
        newSubscriptions.removeAll(sessionSubscriptions);
        if (sessionSubscriptions.size() + newSubscriptions.size() > MAX_SUBSCRIPTIONS_PER_CONNECTION) {
            throw new IllegalArgumentException("单连接最多订阅 " + MAX_SUBSCRIPTIONS_PER_CONNECTION + " 个频道");
        }
        sessionSubscriptions.addAll(newSubscriptions);
        newSubscriptions.forEach(channelKey -> channelSubscribers.computeIfAbsent(channelKey,
                ignored -> new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)).add(channel));
        return Set.copyOf(newSubscriptions);
    }

    /**
     * 为连接移除频道订阅。
     *
     * @param channel 连接
     * @param channelKeys 规范频道名称
     * @return 本次实际移除的订阅
     */
    public Set<String> unsubscribe(Channel channel, Collection<String> channelKeys) {
        Set<String> sessionSubscriptions = requireSession(channel);
        Set<String> removed = new LinkedHashSet<>(channelKeys);
        removed.retainAll(sessionSubscriptions);
        sessionSubscriptions.removeAll(removed);
        removed.forEach(channelKey -> removeFromChannelIndex(channelKey, channel));
        return Set.copyOf(removed);
    }

    /**
     * 将已序列化一次的行情帧扇出给该频道订阅者。
     *
     * @param channelKey 规范频道名称
     * @param message 已序列化 JSON 帧；此方法负责释放其初始引用
     */
    public void publish(String channelKey, TextWebSocketFrame message) {
        try {
            ChannelGroup subscribers = channelSubscribers.get(channelKey);
            if (subscribers == null) {
                return;
            }
            subscribers.forEach(channel -> deliver(channel, message));
        } finally {
            message.release();
        }
    }

    /**
     * 获取当前 WebSocket 连接数。
     *
     * @return 活跃连接数
     */
    public int connectionCount() {
        return channels.size();
    }

    /**
     * 获取指定连接当前订阅，供协议应答与测试读取。
     *
     * @param channel 连接
     * @return 不可修改的订阅集合
     */
    public Set<String> subscriptionsOf(Channel channel) {
        return Set.copyOf(requireSession(channel));
    }

    /**
     * 获取已登记连接的订阅集合。
     *
     * @param channel 连接
     * @return 可并发修改的订阅集合
     */
    private Set<String> requireSession(Channel channel) {
        Set<String> sessionSubscriptions = subscriptions.get(channel.id());
        if (sessionSubscriptions == null) {
            throw new IllegalStateException("WebSocket 连接尚未完成握手");
        }
        return sessionSubscriptions;
    }

    /**
     * 从频道订阅索引移除连接，并在频道无订阅者时回收索引项。
     *
     * @param channelKey 规范频道名称
     * @param channel 连接
     */
    private void removeFromChannelIndex(String channelKey, Channel channel) {
        channelSubscribers.computeIfPresent(channelKey, (key, subscribers) -> {
            subscribers.remove(channel);
            return subscribers.isEmpty() ? null : subscribers;
        });
    }

    /**
     * 向单个连接写入同一序列化帧的引用副本。
     *
     * @param channel 目标连接
     * @param message 已序列化行情帧
     */
    private void deliver(Channel channel, TextWebSocketFrame message) {
        if (!channel.isActive()) {
            unregister(channel);
            return;
        }
        if (!channel.isWritable()) {
            protectSlowConsumer(channel);
            return;
        }
        consecutiveUnwritableCounts.remove(channel.id());
        channel.writeAndFlush(message.retainedDuplicate()).addListener(result -> {
            if (!result.isSuccess()) {
                channel.close();
            }
        });
    }

    /**
     * 记录不可写连接；连续积压达到阈值后主动关闭，避免无限堆积出站缓冲区。
     *
     * @param channel 不可写连接
     */
    private void protectSlowConsumer(Channel channel) {
        int count = consecutiveUnwritableCounts.merge(channel.id(), 1, Integer::sum);
        if (count == maxConsecutiveUnwritable) {
            marketMetrics.recordSlowConsumerDisconnect();
            channel.close();
        }
    }
}
