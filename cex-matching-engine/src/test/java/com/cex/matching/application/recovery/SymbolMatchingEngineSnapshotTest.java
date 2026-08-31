package com.cex.matching.application.recovery;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.matching.domain.model.MatchResult;
import com.cex.matching.domain.model.OrderBookSnapshot;
import com.cex.matching.application.command.MatchingEngineRegistry;
import com.cex.matching.application.command.SymbolMatchingEngine;
import com.cex.matching.application.mapper.OrderEventMapper;
import com.cex.matching.application.port.outbound.MatchingSnapshotRepository;
import com.cex.matching.application.recovery.support.InMemoryMatchingCommandJournal;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class SymbolMatchingEngineSnapshotTest {

    @Test
    void restore_shouldKeepPartiallyFilledRestingOrderAndSequence() {
        AtomicLong tradeIds = new AtomicLong();
        SymbolMatchingEngine engine = new SymbolMatchingEngine("BTC_USDT", new OrderEventMapper(),
                tradeIds::incrementAndGet);
        engine.process(limitEvent("sell-1", "1", OrderEvent.OrderSide.SELL, "1"));
        engine.process(limitEvent("buy-1", "2", OrderEvent.OrderSide.BUY, "0.4"));

        SymbolMatchingEngine restored = SymbolMatchingEngine.restore(engine.snapshot(), new OrderEventMapper(),
                tradeIds::incrementAndGet);
        MatchResult result = restored.process(limitEvent("buy-2", "3", OrderEvent.OrderSide.BUY, "0.6"));

        assertThat(result.getTrades()).hasSize(1);
        assertThat(result.getTrades().get(0).getMakerOrderId()).isEqualTo(1L);
        assertThat(result.getTrades().get(0).getQuantity()).isEqualByComparingTo("0.6");
    }

    @Test
    void processAtSequence_shouldRejectSequenceGap() {
        SymbolMatchingEngine engine = new SymbolMatchingEngine("BTC_USDT", new OrderEventMapper(),
                new AtomicLong()::incrementAndGet);

        assertThatIllegalStateException().isThrownBy(() -> engine.processAtSequence(
                limitEvent("buy-1", "1", OrderEvent.OrderSide.BUY, "1"), 2L));
    }

    @Test
    void recover_shouldLoadSnapshotThenReplayLaterWalCommands() {
        InMemoryMatchingCommandJournal journal = new InMemoryMatchingCommandJournal();
        MatchingEngineRegistry original = new MatchingEngineRegistry(journal);
        original.process(limitEvent("sell-1", "1", OrderEvent.OrderSide.SELL, "1"));
        original.process(limitEvent("buy-1", "2", OrderEvent.OrderSide.BUY, "0.4"));
        MemorySnapshotRepository snapshots = new MemorySnapshotRepository();
        snapshots.save(original.snapshot("BTC_USDT").orElseThrow());
        original.process(limitEvent("buy-2", "3", OrderEvent.OrderSide.BUY, "0.6"));

        MatchingEngineRegistry restored = new MatchingEngineRegistry(journal);
        new MatchingRecoveryService(restored, snapshots, journal).recover("BTC_USDT");

        MatchResult replayedResult = restored.process(limitEvent("buy-2", "3", OrderEvent.OrderSide.BUY, "0.6"))
                .orElseThrow();
        assertThat(replayedResult.getTrades()).hasSize(1);
        assertThat(replayedResult.getTrades().get(0).getQuantity()).isEqualByComparingTo("0.6");
    }

    @Test
    void recover_shouldReturnSnapshotStoredResultWhenKafkaRedeliversCompactedCommand() {
        InMemoryMatchingCommandJournal journal = new InMemoryMatchingCommandJournal();
        MatchingEngineRegistry original = new MatchingEngineRegistry(journal);
        OrderEvent sellEvent = limitEvent("sell-1", "1", OrderEvent.OrderSide.SELL, "1");
        OrderEvent buyEvent = limitEvent("buy-1", "2", OrderEvent.OrderSide.BUY, "1");
        original.process(sellEvent);
        original.process(buyEvent);
        MemorySnapshotRepository snapshots = new MemorySnapshotRepository();
        new MatchingSnapshotService(original, snapshots, journal).saveSnapshot("BTC_USDT");

        MatchingEngineRegistry restored = new MatchingEngineRegistry(journal);
        new MatchingRecoveryService(restored, snapshots, journal).recover("BTC_USDT");

        MatchResult redeliveredResult = restored.process(buyEvent).orElseThrow();
        assertThat(redeliveredResult.getTrades()).hasSize(1);
        assertThat(redeliveredResult.getTrades().get(0).getTradeId()).isNotBlank();
    }

    @Test
    void recover_shouldRestoreTenThousandCommandsFromSnapshotAndWal() {
        InMemoryMatchingCommandJournal journal = new InMemoryMatchingCommandJournal();
        MemorySnapshotRepository snapshots = new MemorySnapshotRepository();
        MatchingEngineRegistry original = new MatchingEngineRegistry(journal);
        MatchingSnapshotService snapshotService = new MatchingSnapshotService(original, snapshots, journal);

        for (int index = 1; index <= 10_000; index++) {
            original.process(limitEvent("buy-" + index, String.valueOf(index), OrderEvent.OrderSide.BUY, "1"));
            if (index == 5_000) {
                snapshotService.saveSnapshot("BTC_USDT");
            }
        }
        OrderBookSnapshot expected = original.snapshot("BTC_USDT").orElseThrow();

        MatchingEngineRegistry restored = new MatchingEngineRegistry(journal);
        new MatchingRecoveryService(restored, snapshots, journal).recover("BTC_USDT");
        OrderBookSnapshot actual = restored.snapshot("BTC_USDT").orElseThrow();

        assertThat(actual.sequence()).isEqualTo(10_000L);
        assertThat(actual.orders()).isEqualTo(expected.orders());
    }

    private OrderEvent limitEvent(String eventId, String orderId, OrderEvent.OrderSide side, String quantity) {
        return OrderEvent.builder().eventId(eventId).orderId(orderId).userId(7L).symbol("BTC_USDT")
                .action(OrderEvent.Action.SUBMIT).side(side).type(OrderEvent.OrderType.LIMIT)
                .price(new BigDecimal("100")).quantity(new BigDecimal(quantity))
                .timeInForce(OrderEvent.TimeInForce.GTC).timestamp(1L).build();
    }

    /** 用于恢复编排测试的内存快照仓库。 */
    private static final class MemorySnapshotRepository implements MatchingSnapshotRepository {

        private final Map<String, OrderBookSnapshot> snapshots = new HashMap<>();

        @Override
        public void save(OrderBookSnapshot snapshot) {
            snapshots.put(snapshot.symbol(), snapshot);
        }

        @Override
        public Optional<OrderBookSnapshot> load(String symbol) {
            return Optional.ofNullable(snapshots.get(symbol));
        }
    }
}
