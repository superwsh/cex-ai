package com.cex.market.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.cex.market.infrastructure.metrics.MarketMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** WebSocket 订阅、取消订阅和心跳协议测试。 */
class MarketWebSocketCommandHandlerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void shouldSubscribeAndAcknowledgeValidChannels() throws Exception {
        MarketWebSocketSessionManager sessionManager = sessionManager();
        MarketWebSocketCommandHandler handler = new MarketWebSocketCommandHandler(objectMapper, sessionManager);
        EmbeddedChannel channel = new EmbeddedChannel();
        sessionManager.register(channel);

        handler.handle(channel, "{\"id\":\"req-1\",\"op\":\"SUBSCRIBE\",\"channels\":[\"trade.BTC_USDT\",\"kline.BTC_USDT.1m\"]}");

        JsonNode response = response(channel);
        assertThat(response.path("type").asText()).isEqualTo("ACK");
        assertThat(response.path("op").asText()).isEqualTo("SUBSCRIBE");
        assertThat(response.path("id").asText()).isEqualTo("req-1");
        assertThat(sessionManager.subscriptionsOf(channel)).containsExactlyInAnyOrder("trade.BTC_USDT", "kline.BTC_USDT.1m");
    }

    @Test
    void shouldUnsubscribeAndReplyPong() throws Exception {
        MarketWebSocketSessionManager sessionManager = sessionManager();
        MarketWebSocketCommandHandler handler = new MarketWebSocketCommandHandler(objectMapper, sessionManager);
        EmbeddedChannel channel = new EmbeddedChannel();
        sessionManager.register(channel);
        handler.handle(channel, "{\"op\":\"SUBSCRIBE\",\"channels\":[\"ticker.BTC_USDT\"]}");
        response(channel);

        handler.handle(channel, "{\"op\":\"UNSUBSCRIBE\",\"channels\":[\"ticker.BTC_USDT\"]}");
        JsonNode unsubscribeResponse = response(channel);
        handler.handle(channel, "{\"op\":\"PING\"}");
        JsonNode pongResponse = response(channel);

        assertThat(unsubscribeResponse.path("op").asText()).isEqualTo("UNSUBSCRIBE");
        assertThat(sessionManager.subscriptionsOf(channel)).isEmpty();
        assertThat(pongResponse.path("type").asText()).isEqualTo("PONG");
        assertThat(pongResponse.path("serverTime").asLong()).isPositive();
    }

    @Test
    void shouldRejectUnsupportedChannel() throws Exception {
        MarketWebSocketSessionManager sessionManager = sessionManager();
        MarketWebSocketCommandHandler handler = new MarketWebSocketCommandHandler(objectMapper, sessionManager);
        EmbeddedChannel channel = new EmbeddedChannel();
        sessionManager.register(channel);

        handler.handle(channel, "{\"op\":\"SUBSCRIBE\",\"channels\":[\"unknown.BTC_USDT\"]}");

        JsonNode response = response(channel);
        assertThat(response.path("type").asText()).isEqualTo("ERROR");
        assertThat(response.path("code").asText()).isEqualTo("INVALID_REQUEST");
    }

    private JsonNode response(EmbeddedChannel channel) throws Exception {
        TextWebSocketFrame frame = channel.readOutbound();
        return objectMapper.readTree(frame.text());
    }

    private MarketWebSocketSessionManager sessionManager() {
        return new MarketWebSocketSessionManager(new MarketMetrics(new SimpleMeterRegistry()));
    }
}
