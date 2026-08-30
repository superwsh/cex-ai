package com.cex.matching.domain.snapshot;

import com.cex.matching.domain.model.MatchOrder;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class MatchingSnapshotTest {

    @Test
    void shouldRejectNonPositiveLastSequence() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MatchingSnapshot(
                "BTC_USDT", 0L, 1L, List.of(), List.of()));
    }

    @Test
    void shouldRejectOrderPlacedInWrongSideList() {
        SnapshotOrder sellOrder = new SnapshotOrder(1L, 2L, MatchOrder.Side.SELL,
                new BigDecimal("1.0"), new BigDecimal("2.0"), new BigDecimal("2.0"), 1L);

        assertThatIllegalArgumentException().isThrownBy(() -> new MatchingSnapshot(
                "BTC_USDT", 1L, 1L, List.of(sellOrder), List.of()));
    }
}
