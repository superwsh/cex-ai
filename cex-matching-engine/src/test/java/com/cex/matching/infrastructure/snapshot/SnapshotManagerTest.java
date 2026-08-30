package com.cex.matching.infrastructure.snapshot;

import com.cex.matching.domain.snapshot.MatchingSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class SnapshotManagerTest {
    @TempDir Path root;
    @Test
    void shouldIsolateSnapshotsBySymbol() {
        SnapshotManager manager = new SnapshotManager(root, new SnapshotWriter(new SnapshotCodec()), new SnapshotReader(new SnapshotCodec()));
        manager.save(snapshot("BTC_USDT", 1L));
        manager.save(snapshot("ETH_USDT", 2L));
        assertThat(manager.loadLatest("BTC_USDT")).contains(snapshot("BTC_USDT", 1L));
        assertThat(manager.loadLatest("ETH_USDT")).contains(snapshot("ETH_USDT", 2L));
    }
    @Test
    void shouldRejectUnsafeSymbol() {
        SnapshotManager manager = new SnapshotManager(root, new SnapshotWriter(new SnapshotCodec()), new SnapshotReader(new SnapshotCodec()));
        assertThatIllegalArgumentException().isThrownBy(() -> manager.loadLatest("../BTC"));
    }
    private MatchingSnapshot snapshot(String symbol, long sequence) { return new MatchingSnapshot(symbol, sequence, 1L, List.of(), List.of()); }
}
