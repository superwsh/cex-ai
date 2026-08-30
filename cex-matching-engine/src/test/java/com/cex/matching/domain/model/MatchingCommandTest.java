package com.cex.matching.domain.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class MatchingCommandTest {

    @Test
    void rejectsNonPositiveIntegerAmountForNewOrder() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MatchingCommand(
                1L, "command-1", "1", "10", "BTC_USDT", CommandType.NEW_ORDER,
                MatchOrder.Side.BUY, 0L, 100_000_000L, 1_700_000_000_000L));
    }

    @Test
    void allowsCancelCommandWithoutSideOrAmounts() {
        MatchingCommand command = new MatchingCommand(
                1L, "command-2", "1", "10", "BTC_USDT", CommandType.CANCEL_ORDER,
                null, 0L, 0L, 1_700_000_000_000L);

        assertThat(command.commandType()).isEqualTo(CommandType.CANCEL_ORDER);
    }

    @Test
    void rejectsNonNumericOrderOrUserIdentity() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MatchingCommand(
                1L, "command-3", "invalid", "10", "BTC_USDT", CommandType.NEW_ORDER,
                MatchOrder.Side.BUY, 100L, 100L, 1_700_000_000_000L));
        assertThatIllegalArgumentException().isThrownBy(() -> new MatchingCommand(
                1L, "command-4", "1", "invalid", "BTC_USDT", CommandType.NEW_ORDER,
                MatchOrder.Side.BUY, 100L, 100L, 1_700_000_000_000L));
    }
}
