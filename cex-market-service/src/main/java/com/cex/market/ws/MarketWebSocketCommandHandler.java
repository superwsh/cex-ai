package com.cex.market.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** 处理客户端 WebSocket 心跳及订阅协议。 */
@Component
@RequiredArgsConstructor
public class MarketWebSocketCommandHandler {

    private final ObjectMapper objectMapper;
    private final MarketWebSocketSessionManager sessionManager;

    /**
     * 处理一帧客户端文本消息，并返回协议应答。
     *
     * @param channel 已完成握手的 WebSocket 连接
     * @param text 客户端 JSON 文本
     */
    public void handle(Channel channel, String text) {
        JsonNode request = null;
        try {
            request = objectMapper.readTree(text);
            if (request == null || !request.isObject()) {
                throw new IllegalArgumentException("请求必须是 JSON 对象");
            }
            String operation = operation(request);
            switch (operation) {
                case "SUBSCRIBE" -> subscribe(channel, request);
                case "UNSUBSCRIBE" -> unsubscribe(channel, request);
                case "PING" -> pong(channel, request);
                default -> throw new IllegalArgumentException("不支持的操作: " + operation);
            }
        } catch (JsonProcessingException exception) {
            error(channel, null, "INVALID_JSON", "请求不是合法 JSON");
        } catch (IllegalArgumentException | IllegalStateException exception) {
            error(channel, request == null ? null : requestId(request), "INVALID_REQUEST", exception.getMessage());
        }
    }

    /**
     * 处理订阅请求。
     *
     * @param channel WebSocket 连接
     * @param request 已解析 JSON 请求
     */
    private void subscribe(Channel channel, JsonNode request) {
        List<String> channelKeys = parseChannels(request);
        Set<String> subscribed = sessionManager.subscribe(channel, channelKeys);
        acknowledgement(channel, request, "SUBSCRIBE", subscribed);
    }

    /**
     * 处理取消订阅请求。
     *
     * @param channel WebSocket 连接
     * @param request 已解析 JSON 请求
     */
    private void unsubscribe(Channel channel, JsonNode request) {
        List<String> channelKeys = parseChannels(request);
        Set<String> unsubscribed = sessionManager.unsubscribe(channel, channelKeys);
        acknowledgement(channel, request, "UNSUBSCRIBE", unsubscribed);
    }

    /**
     * 返回应用层心跳响应。
     *
     * @param channel WebSocket 连接
     * @param request 已解析 JSON 请求
     */
    private void pong(Channel channel, JsonNode request) {
        ObjectNode response = baseResponse("PONG", requestId(request));
        response.put("serverTime", System.currentTimeMillis());
        write(channel, response);
    }

    /**
     * 解析并校验客户端请求的频道数组。
     *
     * @param request 已解析 JSON 请求
     * @return 规范频道名称
     */
    private List<String> parseChannels(JsonNode request) {
        JsonNode channels = request.get("channels");
        if (channels == null || !channels.isArray() || channels.isEmpty()) {
            throw new IllegalArgumentException("channels 必须是非空数组");
        }
        List<String> channelKeys = new ArrayList<>();
        for (JsonNode item : channels) {
            if (!item.isTextual()) {
                throw new IllegalArgumentException("channels 必须只包含字符串");
            }
            channelKeys.add(MarketWebSocketChannel.parse(item.asText()).key());
        }
        return List.copyOf(channelKeys);
    }

    /**
     * 读取并标准化请求操作名称。
     *
     * @param request 已解析 JSON 请求
     * @return 大写操作名称
     */
    private String operation(JsonNode request) {
        JsonNode operation = request.hasNonNull("op") ? request.get("op") : request.get("type");
        if (operation == null || !operation.isTextual() || operation.asText().isBlank()) {
            throw new IllegalArgumentException("op 不能为空");
        }
        return operation.asText().toUpperCase(Locale.ROOT);
    }

    /**
     * 返回订阅或取消订阅确认。
     *
     * @param channel WebSocket 连接
     * @param request 原始请求
     * @param operation 已执行操作
     * @param channelKeys 本次变更的频道
     */
    private void acknowledgement(Channel channel, JsonNode request, String operation, Set<String> channelKeys) {
        ObjectNode response = baseResponse("ACK", requestId(request));
        response.put("op", operation);
        response.set("channels", objectMapper.valueToTree(channelKeys));
        write(channel, response);
    }

    /**
     * 返回客户端协议错误。
     *
     * @param channel WebSocket 连接
     * @param requestId 请求标识，可为空
     * @param code 错误码
     * @param message 错误说明
     */
    private void error(Channel channel, String requestId, String code, String message) {
        ObjectNode response = baseResponse("ERROR", requestId);
        response.put("code", code);
        response.put("message", message == null ? "请求处理失败" : message);
        write(channel, response);
    }

    /**
     * 构造包含公共字段的协议应答。
     *
     * @param type 应答类型
     * @param requestId 请求标识，可为空
     * @return JSON 对象
     */
    private ObjectNode baseResponse(String type, String requestId) {
        ObjectNode response = objectMapper.createObjectNode();
        response.put("type", type);
        if (requestId != null) {
            response.put("id", requestId);
        }
        return response;
    }

    /**
     * 读取可选请求标识。
     *
     * @param request 原始请求
     * @return 请求标识；未提供时为空
     */
    private String requestId(JsonNode request) {
        JsonNode requestId = request.get("id");
        return requestId != null && requestId.isTextual() ? requestId.asText() : null;
    }

    /**
     * 序列化并写出协议消息。
     *
     * @param channel WebSocket 连接
     * @param response 响应 JSON 对象
     */
    private void write(Channel channel, ObjectNode response) {
        try {
            channel.writeAndFlush(new TextWebSocketFrame(objectMapper.writeValueAsString(response)));
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("WebSocket 响应序列化失败", exception);
        }
    }
}
