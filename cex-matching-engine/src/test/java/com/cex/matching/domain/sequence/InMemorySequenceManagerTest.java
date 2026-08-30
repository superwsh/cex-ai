package com.cex.matching.domain.sequence;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemorySequenceManagerTest {

    private final SequenceManager sequenceManager = new InMemorySequenceManager();

    @Test
    void acceptsFirstSequenceAndAdvancesIt() {
        assertThat(sequenceManager.current("BTC_USDT")).isZero();
        assertThat(sequenceManager.validate("BTC_USDT", 1L)).isEqualTo(SequenceValidation.ACCEPTED);

        sequenceManager.advance("BTC_USDT", 1L);

        assertThat(sequenceManager.current("BTC_USDT")).isEqualTo(1L);
    }

    @Test
    void acceptsConsecutiveSequence() {
        sequenceManager.advance("BTC_USDT", 1L);

        assertThat(sequenceManager.validate("BTC_USDT", 2L)).isEqualTo(SequenceValidation.ACCEPTED);
    }

    @Test
    void returnsDuplicateWithoutAdvancingForCurrentOrEarlierSequence() {
        sequenceManager.advance("BTC_USDT", 1L);
        sequenceManager.advance("BTC_USDT", 2L);

        assertThat(sequenceManager.validate("BTC_USDT", 2L)).isEqualTo(SequenceValidation.DUPLICATE);
        assertThat(sequenceManager.validate("BTC_USDT", 1L)).isEqualTo(SequenceValidation.DUPLICATE);
        assertThat(sequenceManager.current("BTC_USDT")).isEqualTo(2L);
    }

    @Test
    void rejectsSequenceGap() {
        sequenceManager.advance("BTC_USDT", 1L);

        assertThatThrownBy(() -> sequenceManager.validate("BTC_USDT", 3L))
                .isInstanceOf(SequenceGapException.class);
    }

    @Test
    void isolatesSequencesBySymbol() {
        sequenceManager.advance("BTC_USDT", 1L);
        sequenceManager.advance("BTC_USDT", 2L);
        sequenceManager.advance("BTC_USDT", 3L);

        assertThat(sequenceManager.current("ETH_USDT")).isZero();
        assertThat(sequenceManager.validate("ETH_USDT", 1L)).isEqualTo(SequenceValidation.ACCEPTED);
    }

    @Test
    void advancesOnlyTheNextConsecutiveSequence() {
        assertThatIllegalStateException().isThrownBy(() -> sequenceManager.advance("BTC_USDT", 2L));

        sequenceManager.advance("BTC_USDT", 1L);

        assertThatIllegalStateException().isThrownBy(() -> sequenceManager.advance("BTC_USDT", 1L));
        assertThatIllegalStateException().isThrownBy(() -> sequenceManager.advance("BTC_USDT", 3L));
        assertThat(sequenceManager.current("BTC_USDT")).isEqualTo(1L);
    }

    @Test
    void restoresSequenceWatermarkForRecovery() {
        sequenceManager.restore("BTC_USDT", 7L);

        assertThat(sequenceManager.current("BTC_USDT")).isEqualTo(7L);
        assertThat(sequenceManager.validate("BTC_USDT", 8L)).isEqualTo(SequenceValidation.ACCEPTED);
    }
}
