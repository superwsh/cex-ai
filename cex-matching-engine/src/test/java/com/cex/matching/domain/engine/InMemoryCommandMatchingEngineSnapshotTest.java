package com.cex.matching.domain.engine;

import com.cex.matching.application.CommandNormalizer;
import com.cex.matching.application.DecimalScale;
import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.snapshot.MatchingSnapshot;
import com.cex.matching.domain.snapshot.SnapshotOrder;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

class InMemoryCommandMatchingEngineSnapshotTest {
    @Test
    void shouldRestoreActiveOrdersFromSnapshot() {
        InMemoryCommandMatchingEngine engine = new InMemoryCommandMatchingEngine(new CommandNormalizer(Map.of("BTC_USDT", new DecimalScale(2, 4))));
        MatchingSnapshot snapshot = new MatchingSnapshot("BTC_USDT", 5L, 1L, List.of(new SnapshotOrder(11L, 7L, MatchOrder.Side.BUY, new BigDecimal("1.23"), BigDecimal.TEN, BigDecimal.TEN, 5L)), List.of());
        engine.restore(snapshot);
        assertThat(engine.findOrder("BTC_USDT", 11L)).isPresent();
    }
}
