package com.cex.matching.infrastructure.snapshot;

import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.snapshot.MatchingSnapshot;
import com.cex.matching.domain.snapshot.SnapshotOrder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotReaderWriterTest {

    @TempDir
    Path root;

    @Test
    void shouldAtomicallyWriteAndReadSnapshot() throws Exception {
        SnapshotWriter writer = new SnapshotWriter(new SnapshotCodec());
        SnapshotReader reader = new SnapshotReader(new SnapshotCodec());
        Path file = root.resolve("snapshot-8.json");
        MatchingSnapshot snapshot = snapshot(8L);

        writer.write(file, snapshot);

        assertThat(reader.read(file)).isEqualTo(snapshot);
        try (Stream<Path> paths = Files.list(root)) {
            assertThat(paths.map(path -> path.getFileName().toString()))
                    .noneMatch(name -> name.endsWith(".tmp"));
        }
    }

    @Test
    void shouldFallbackToOlderValidSnapshotWhenLatestFileIsCorrupted() throws Exception {
        SnapshotWriter writer = new SnapshotWriter(new SnapshotCodec());
        SnapshotReader reader = new SnapshotReader(new SnapshotCodec());
        writer.write(root.resolve("snapshot-7.json"), snapshot(7L));
        Files.writeString(root.resolve("snapshot-8.json"), "损坏内容");

        assertThat(reader.readLatest(root)).contains(snapshot(7L));
    }

    private MatchingSnapshot snapshot(long sequence) {
        SnapshotOrder buy = new SnapshotOrder(1L, 2L, MatchOrder.Side.BUY,
                new BigDecimal("10.00"), new BigDecimal("2.0"), new BigDecimal("1.5"), sequence);
        return new MatchingSnapshot("BTC_USDT", sequence, 1000L, List.of(buy), List.of());
    }
}
