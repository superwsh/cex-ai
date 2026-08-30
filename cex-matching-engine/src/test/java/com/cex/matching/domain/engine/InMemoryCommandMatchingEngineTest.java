package com.cex.matching.domain.engine;

import com.cex.matching.application.CommandNormalizer;
import com.cex.matching.application.DecimalScale;
import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.model.MatchingCommand;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryCommandMatchingEngineTest {

    @Test
    void shouldAddNormalizedOrderForNewOrderCommand() {
        InMemoryCommandMatchingEngine engine = new InMemoryCommandMatchingEngine(normalizer());

        engine.execute(newOrder(1L, "11"), ExecutionMode.LIVE);

        assertThat(engine.findOrder("BTC_USDT", 11L)).isPresent();
    }

    @Test
    void shouldRemoveExistingOrderForCancelCommand() {
        InMemoryCommandMatchingEngine engine = new InMemoryCommandMatchingEngine(normalizer());
        engine.execute(newOrder(1L, "11"), ExecutionMode.LIVE);

        engine.execute(cancelOrder(2L, "11"), ExecutionMode.LIVE);

        assertThat(engine.findOrder("BTC_USDT", 11L)).isEmpty();
    }

    @Test
    void shouldTreatMissingCancelAsIdempotentSuccess() {
        InMemoryCommandMatchingEngine engine = new InMemoryCommandMatchingEngine(normalizer());

        engine.execute(cancelOrder(1L, "99"), ExecutionMode.LIVE);

        assertThat(engine.findOrder("BTC_USDT", 99L)).isEmpty();
    }

    private CommandNormalizer normalizer() {
        return new CommandNormalizer(Map.of("BTC_USDT", new DecimalScale(2, 4)));
    }

    private MatchingCommand newOrder(long sequence, String orderId) {
        return new MatchingCommand(sequence, "command-" + sequence, orderId, "7", "BTC_USDT",
                CommandType.NEW_ORDER, MatchOrder.Side.BUY, 12345L, 25000L, 1000L + sequence);
    }

    private MatchingCommand cancelOrder(long sequence, String orderId) {
        return new MatchingCommand(sequence, "command-" + sequence, orderId, "7", "BTC_USDT",
                CommandType.CANCEL_ORDER, null, 0L, 0L, 1000L + sequence);
    }
}
