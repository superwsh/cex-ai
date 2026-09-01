package com.cex.market.ws;

import com.cex.common.kafka.event.TradeEvent;
import com.cex.market.domain.trade.MarketTrade;
import com.cex.market.domain.trade.Ticker24h;
import com.cex.market.infrastructure.metrics.MarketMetrics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** WebSocket 行情频道分发测试。 */
class MarketWebSocketPublisherTest {

    @Test
    void shouldPublishTradeOnlyToSubscribedConnection() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        MarketWebSocketSessionManager sessionManager = sessionManager();
        MarketWebSocketBatchDispatcher dispatcher = new MarketWebSocketBatchDispatcher(objectMapper, sessionManager);
        MarketWebSocketPublisher publisher = new MarketWebSocketPublisher(dispatcher);
        EmbeddedChannel subscribed = new EmbeddedChannel();
        EmbeddedChannel notSubscribed = new EmbeddedChannel();
        sessionManager.register(subscribed);
        sessionManager.register(notSubscribed);
        sessionManager.subscribe(subscribed, List.of("trade.BTC_USDT"));

        publisher.publishTrade(new MarketTrade("trade-1", "BTC_USDT", new BigDecimal("100"), new BigDecimal("2"),
                new BigDecimal("200"), TradeEvent.TakerSide.BUY, 1_700_000_000_000L));
        dispatcher.flush();

        TextWebSocketFrame frame = subscribed.readOutbound();
        JsonNode response = objectMapper.readTree(frame.text());
        assertThat(response.path("type").asText()).isEqualTo("TRADE_BATCH");
        assertThat(response.path("channel").asText()).isEqualTo("trade.BTC_USDT");
        assertThat(response.path("data").get(0).path("tradeId").asText()).isEqualTo("trade-1");
        assertThat((Object) notSubscribed.readOutbound()).isNull();
    }

    @Test
    void shouldConflateTickerToLatestSnapshotWithinFlushInterval() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        MarketWebSocketSessionManager sessionManager = sessionManager();
        MarketWebSocketBatchDispatcher dispatcher = new MarketWebSocketBatchDispatcher(objectMapper, sessionManager);
        MarketWebSocketPublisher publisher = new MarketWebSocketPublisher(dispatcher);
        EmbeddedChannel subscribed = new EmbeddedChannel();
        sessionManager.register(subscribed);
        sessionManager.subscribe(subscribed, List.of("ticker.BTC_USDT"));

        publisher.publishTicker(ticker("100"));
        publisher.publishTicker(ticker("101"));
        dispatcher.flush();

        TextWebSocketFrame frame = subscribed.readOutbound();
        JsonNode response = objectMapper.readTree(frame.text());
        assertThat(response.path("type").asText()).isEqualTo("TICKER");
        assertThat(response.path("data").path("lastPrice").decimalValue()).isEqualByComparingTo("101");
        assertThat((Object) subscribed.readOutbound()).isNull();
    }

    private Ticker24h ticker(String lastPrice) {
        return new Ticker24h("BTC_USDT", new BigDecimal(lastPrice), new BigDecimal("90"), new BigDecimal("110"),
                new BigDecimal("80"), new BigDecimal("10"), new BigDecimal("11.11"), new BigDecimal("12"),
                new BigDecimal("1200"), 2L, 1_699_913_600_000L, 1_700_000_000_000L);
    }

    private MarketWebSocketSessionManager sessionManager() {
        return new MarketWebSocketSessionManager(new MarketMetrics(new SimpleMeterRegistry()));
    }
}
