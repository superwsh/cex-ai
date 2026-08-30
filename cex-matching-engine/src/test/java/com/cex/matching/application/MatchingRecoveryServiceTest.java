package com.cex.matching.application;

import com.cex.matching.domain.engine.InMemoryCommandMatchingEngine;
import com.cex.matching.domain.engine.ExecutionMode;
import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.model.MatchingCommand;
import com.cex.matching.domain.sequence.InMemorySequenceManager;
import com.cex.matching.domain.snapshot.MatchingSnapshot;
import com.cex.matching.domain.snapshot.SnapshotOrder;
import com.cex.matching.infrastructure.snapshot.SnapshotCodec;
import com.cex.matching.infrastructure.snapshot.SnapshotManager;
import com.cex.matching.infrastructure.snapshot.SnapshotReader;
import com.cex.matching.infrastructure.snapshot.SnapshotWriter;
import com.cex.matching.infrastructure.wal.WalReadResult;
import com.cex.matching.infrastructure.wal.WalReader;
import com.cex.matching.infrastructure.wal.WalRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MatchingRecoveryServiceTest {

    @TempDir
    Path root;

    @Test
    void shouldClearExistingBookAndReplayAllWalWhenSnapshotIsAbsent() {
        InMemorySequenceManager sequenceManager = new InMemorySequenceManager();
        InMemoryCommandMatchingEngine engine = engine();
        engine.execute(newOrder(99L, "999"), ExecutionMode.LIVE);
        List<WalRecord> appendedRecords = new ArrayList<>();
        MatchingCommandHandler handler = handler(sequenceManager, engine, appendedRecords);
        MatchingRecoveryService recoveryService = recoveryService(engine, handler, sequenceManager,
                symbol -> new WalReadResult(List.of(WalRecord.from(newOrder(1L, "11")),
                        WalRecord.from(newOrder(2L, "12"))), false));

        recoveryService.recover("BTC_USDT");

        assertThat(engine.findOrder("BTC_USDT", 999L)).isEmpty();
        assertThat(engine.findOrder("BTC_USDT", 11L)).isPresent();
        assertThat(engine.findOrder("BTC_USDT", 12L)).isPresent();
        assertThat(sequenceManager.current("BTC_USDT")).isEqualTo(2L);
        assertThat(appendedRecords).isEmpty();
    }

    @Test
    void shouldRestoreSnapshotAndReplayOnlySortedRecordsAfterSnapshotSequence() {
        InMemorySequenceManager sequenceManager = new InMemorySequenceManager();
        InMemoryCommandMatchingEngine engine = engine();
        List<WalRecord> appendedRecords = new ArrayList<>();
        MatchingCommandHandler handler = handler(sequenceManager, engine, appendedRecords);
        snapshots().save(snapshotAt(2L, "11"));
        MatchingRecoveryService recoveryService = recoveryService(engine, handler, sequenceManager,
                symbol -> new WalReadResult(List.of(WalRecord.from(newOrder(4L, "13")),
                        WalRecord.from(newOrder(1L, "9")), WalRecord.from(newOrder(3L, "12")),
                        WalRecord.from(newOrder(2L, "10"))), false));

        recoveryService.recover("BTC_USDT");

        assertThat(engine.findOrder("BTC_USDT", 11L)).isPresent();
        assertThat(engine.findOrder("BTC_USDT", 12L)).isPresent();
        assertThat(engine.findOrder("BTC_USDT", 13L)).isPresent();
        assertThat(engine.findOrder("BTC_USDT", 9L)).isEmpty();
        assertThat(engine.findOrder("BTC_USDT", 10L)).isEmpty();
        assertThat(sequenceManager.current("BTC_USDT")).isEqualTo(4L);
        assertThat(appendedRecords).isEmpty();
    }

    @Test
    void shouldKeepSymbolBlockedWhenReplayFails() {
        InMemorySequenceManager sequenceManager = new InMemorySequenceManager();
        InMemoryCommandMatchingEngine engine = engine();
        List<WalRecord> appendedRecords = new ArrayList<>();
        MatchingCommandHandler handler = handler(sequenceManager, engine, appendedRecords);
        WalRecord invalidRecord = new WalRecord(1L, "command-1", "11", "7", "BTC_USDT",
                CommandType.NEW_ORDER, null, 0L, 0L, 1001L, 0L);
        MatchingRecoveryService recoveryService = recoveryService(engine, handler, sequenceManager,
                symbol -> new WalReadResult(List.of(invalidRecord), false));

        assertThatThrownBy(() -> recoveryService.recover("BTC_USDT"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> handler.handle(newOrder(1L, "11")))
                .isInstanceOf(CommandRecoveryRequiredException.class);
        assertThat(sequenceManager.current("BTC_USDT")).isZero();
        assertThat(appendedRecords).isEmpty();
    }

    @Test
    void shouldKeepSymbolBlockedWhenSnapshotDoesNotBelongToRecoveredSymbol() {
        SnapshotCodec codec = new SnapshotCodec();
        new SnapshotWriter(codec).write(root.resolve("BTC_USDT").resolve("snapshot-2.json"),
                new MatchingSnapshot("ETH_USDT", 2L, 1L, List.of(), List.of()));
        InMemorySequenceManager sequenceManager = new InMemorySequenceManager();
        InMemoryCommandMatchingEngine engine = engine();
        List<WalRecord> appendedRecords = new ArrayList<>();
        MatchingCommandHandler handler = handler(sequenceManager, engine, appendedRecords);
        MatchingRecoveryService recoveryService = recoveryService(engine, handler, sequenceManager,
                symbol -> new WalReadResult(List.of(), false));

        assertThatThrownBy(() -> recoveryService.recover("BTC_USDT"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> handler.handle(newOrder(1L, "11")))
                .isInstanceOf(CommandRecoveryRequiredException.class);
        assertThat(sequenceManager.current("BTC_USDT")).isZero();
        assertThat(appendedRecords).isEmpty();
    }

    @Test
    void shouldSerializeConcurrentRecoveryForSameSymbol() throws Exception {
        CountDownLatch firstReadStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstRead = new CountDownLatch(1);
        CountDownLatch secondReadStarted = new CountDownLatch(1);
        InMemorySequenceManager sequenceManager = new InMemorySequenceManager();
        InMemoryCommandMatchingEngine engine = engine();
        MatchingCommandHandler handler = handler(sequenceManager, engine, new ArrayList<>());
        WalReader walReader = symbol -> {
            if (firstReadStarted.getCount() > 0L) {
                firstReadStarted.countDown();
                await(releaseFirstRead);
            } else {
                secondReadStarted.countDown();
            }
            return new WalReadResult(List.of(), false);
        };
        MatchingRecoveryService recoveryService = recoveryService(engine, handler, sequenceManager, walReader);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> firstRecovery = executor.submit(() -> recoveryService.recover("BTC_USDT"));
            assertThat(firstReadStarted.await(1L, TimeUnit.SECONDS)).isTrue();
            Future<?> secondRecovery = executor.submit(() -> recoveryService.recover("BTC_USDT"));

            assertThat(secondReadStarted.await(300L, TimeUnit.MILLISECONDS)).isFalse();

            releaseFirstRead.countDown();
            firstRecovery.get(1L, TimeUnit.SECONDS);
            secondRecovery.get(1L, TimeUnit.SECONDS);
        } finally {
            releaseFirstRead.countDown();
            executor.shutdownNow();
        }
    }

    private MatchingRecoveryService recoveryService(InMemoryCommandMatchingEngine engine,
                                                    MatchingCommandHandler handler,
                                                    InMemorySequenceManager sequenceManager,
                                                    WalReader walReader) {
        return new MatchingRecoveryService(snapshots(), walReader, engine, handler, sequenceManager);
    }

    private SnapshotManager snapshots() {
        SnapshotCodec codec = new SnapshotCodec();
        return new SnapshotManager(root, new SnapshotWriter(codec), new SnapshotReader(codec));
    }

    private MatchingCommandHandler handler(InMemorySequenceManager sequenceManager,
                                           InMemoryCommandMatchingEngine engine,
                                           List<WalRecord> appendedRecords) {
        return new MatchingCommandHandler(sequenceManager, appendedRecords::add, engine);
    }

    private InMemoryCommandMatchingEngine engine() {
        return new InMemoryCommandMatchingEngine(
                new CommandNormalizer(Map.of("BTC_USDT", new DecimalScale(2, 4))));
    }

    private MatchingSnapshot snapshotAt(long sequence, String orderId) {
        return new MatchingSnapshot("BTC_USDT", sequence, 1L,
                List.of(new SnapshotOrder(Long.parseLong(orderId), 7L, MatchOrder.Side.BUY,
                        new BigDecimal("123.45"), new BigDecimal("2.5000"),
                        new BigDecimal("2.5000"), sequence)), List.of());
    }

    private MatchingCommand newOrder(long sequence, String orderId) {
        return new MatchingCommand(sequence, "command-" + sequence, orderId, "7", "BTC_USDT",
                CommandType.NEW_ORDER, MatchOrder.Side.BUY, 12345L, 25000L, 1000L + sequence);
    }

    private void await(CountDownLatch latch) {
        try {
            if (!latch.await(1L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("等待并发测试信号超时");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("等待并发测试信号被中断", exception);
        }
    }
}
