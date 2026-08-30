package com.cex.matching.application;

import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.model.MatchingCommand;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CommandNormalizerTest {

    @Test
    void convertsIntegerAmountsWithConfiguredScalesWithoutPrecisionLoss() {
        CommandNormalizer normalizer = new CommandNormalizer(Map.of(
                "BTC_USDT", new DecimalScale(2, 8)));
        MatchingCommand command = newOrder("42", "6500012", "123456789");

        MatchOrder order = normalizer.toMatchOrder(command);

        assertThat(order.price()).isEqualByComparingTo("65000.12");
        assertThat(order.quantity()).isEqualByComparingTo("1.23456789");
        assertThat(order.remainingQuantity()).isEqualByComparingTo("1.23456789");
    }

    private static MatchingCommand newOrder(String orderId, String price, String quantity) {
        return new MatchingCommand(
                7L, "command-1", orderId, "10", "BTC_USDT", CommandType.NEW_ORDER,
                MatchOrder.Side.BUY, Long.parseLong(price), Long.parseLong(quantity),
                1_700_000_000_000L);
    }
}
