package com.cex.matching.application;

import com.cex.matching.domain.engine.InMemoryCommandMatchingEngine;
import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.model.MatchingCommand;
import com.cex.matching.domain.sequence.InMemorySequenceManager;
import com.cex.matching.infrastructure.wal.WalAppender;
import com.cex.matching.infrastructure.wal.WalException;
import com.cex.matching.infrastructure.wal.WalRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MatchingCommandHandlerTest {

    @Test
    void shouldPersistExecuteAndAdvanceForAcceptedCommand() {
        InMemorySequenceManager sequenceManager = new InMemorySequenceManager();
        InMemoryCommandMatchingEngine engine = engine();
        List<WalRecord> appendedRecords = new ArrayList<>();
        MatchingCommandHandler handler = handler(sequenceManager, engine, appendedRecords::add);

        CommandHandlingResult result = handler.handle(newOrder(1L, "11"));

        assertThat(result).isEqualTo(CommandHandlingResult.APPLIED);
        assertThat(sequenceManager.current("BTC_USDT")).isEqualTo(1L);
        assertThat(engine.findOrder("BTC_USDT", 11L)).isPresent();
        assertThat(appendedRecords).extracting(WalRecord::sequence).containsExactly(1L);
    }

    @Test
    void shouldNotAppendExecuteOrAdvanceDuplicateCommand() {
        InMemorySequenceManager sequenceManager = new InMemorySequenceManager();
        InMemoryCommandMatchingEngine engine = engine();
        List<WalRecord> appendedRecords = new ArrayList<>();
        MatchingCommandHandler handler = handler(sequenceManager, engine, appendedRecords::add);
        handler.handle(newOrder(1L, "11"));

        CommandHandlingResult result = handler.handle(newOrder(1L, "12"));

        assertThat(result).isEqualTo(CommandHandlingResult.DUPLICATE);
        assertThat(sequenceManager.current("BTC_USDT")).isEqualTo(1L);
        assertThat(engine.findOrder("BTC_USDT", 12L)).isEmpty();
        assertThat(appendedRecords).extracting(WalRecord::orderId).containsExactly("11");
    }

    @Test
    void shouldNotExecuteOrAdvanceWhenWalAppendFails() {
        InMemorySequenceManager sequenceManager = new InMemorySequenceManager();
        InMemoryCommandMatchingEngine engine = engine();
        MatchingCommandHandler handler = handler(sequenceManager, engine, record -> {
            throw new WalException("写入失败");
        });

        assertThatThrownBy(() -> handler.handle(newOrder(1L, "11")))
                .isInstanceOf(WalException.class);

        assertThat(sequenceManager.current("BTC_USDT")).isZero();
        assertThat(engine.findOrder("BTC_USDT", 11L)).isEmpty();
    }

    @Test
    void shouldApplyExistingAndMissingCancelCommands() {
        InMemorySequenceManager sequenceManager = new InMemorySequenceManager();
        InMemoryCommandMatchingEngine engine = engine();
        MatchingCommandHandler handler = handler(sequenceManager, engine, record -> { });
        handler.handle(newOrder(1L, "11"));

        handler.handle(cancelOrder(2L, "11"));
        handler.handle(cancelOrder(3L, "99"));

        assertThat(engine.findOrder("BTC_USDT", 11L)).isEmpty();
        assertThat(engine.findOrder("BTC_USDT", 99L)).isEmpty();
        assertThat(sequenceManager.current("BTC_USDT")).isEqualTo(3L);
    }

    @Test
    void shouldNotAppendOrAdvanceWhenReplayingCommand() {
        InMemorySequenceManager sequenceManager = new InMemorySequenceManager();
        InMemoryCommandMatchingEngine engine = engine();
        List<WalRecord> appendedRecords = new ArrayList<>();
        MatchingCommandHandler handler = handler(sequenceManager, engine, appendedRecords::add);

        CommandHandlingResult result = handler.replay(newOrder(8L, "11"));

        assertThat(result).isEqualTo(CommandHandlingResult.APPLIED);
        assertThat(engine.findOrder("BTC_USDT", 11L)).isPresent();
        assertThat(sequenceManager.current("BTC_USDT")).isZero();
        assertThat(appendedRecords).isEmpty();
    }

    private MatchingCommandHandler handler(InMemorySequenceManager sequenceManager,
                                           InMemoryCommandMatchingEngine engine,
                                           WalAppender walAppender) {
        return new MatchingCommandHandler(sequenceManager, walAppender, engine);
    }

    private InMemoryCommandMatchingEngine engine() {
        return new InMemoryCommandMatchingEngine(
                new CommandNormalizer(Map.of("BTC_USDT", new DecimalScale(2, 4))));
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
