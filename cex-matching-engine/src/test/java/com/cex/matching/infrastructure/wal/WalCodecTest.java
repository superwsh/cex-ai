package com.cex.matching.infrastructure.wal;

import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.model.MatchingCommand;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WalCodecTest {

    @Test
    void encodesAndDecodesCommandWithStableChecksum() {
        WalCodec codec = new WalCodec();
        WalRecord record = WalRecord.from(command(101L));

        String line = codec.encode(record);
        WalRecord decoded = codec.decode(line);

        assertThat(line).contains("\"sequence\":101");
        assertThat(decoded.sequence()).isEqualTo(record.sequence());
        assertThat(decoded.commandId()).isEqualTo(record.commandId());
        assertThat(decoded.checksum()).isPositive();
        assertThat(codec.encode(decoded)).isEqualTo(line);
    }

    @Test
    void rejectsLineWithChangedChecksumProtectedContent() {
        WalCodec codec = new WalCodec();
        String validLine = codec.encode(WalRecord.from(command(101L)));

        assertThatThrownBy(() -> codec.decode(validLine.replace("6500012", "6500013")))
                .isInstanceOf(WalCorruptionException.class);
    }

    @Test
    void encodesNullSideAsJsonNull() {
        WalCodec codec = new WalCodec();
        String line = codec.encode(WalRecord.from(new MatchingCommand(
                102L, "cmd-102", "102", "202", "BTCUSDT",
                CommandType.CANCEL_ORDER, null, 0L, 0L, 1700000000000L)));

        assertThat(line).contains("\"side\":null");
        assertThat(codec.decode(line).side()).isNull();
    }

    @Test
    void rejectsMalformedOrIncompleteLine() {
        WalCodec codec = new WalCodec();

        assertThatThrownBy(() -> codec.decode("not-json"))
                .isInstanceOf(WalCorruptionException.class);
        assertThatThrownBy(() -> codec.decode("{\"sequence\":1}"))
                .isInstanceOf(WalCorruptionException.class);
    }

    @Test
    void rejectsValidLineWithTrailingContent() {
        WalCodec codec = new WalCodec();
        String validLine = codec.encode(WalRecord.from(command(101L)));

        assertThatThrownBy(() -> codec.decode(validLine + " trailing"))
                .isInstanceOf(WalCorruptionException.class);
    }

    private static MatchingCommand command(long sequence) {
        return new MatchingCommand(sequence, "cmd-" + sequence, String.valueOf(sequence),
                "202", "BTCUSDT", CommandType.NEW_ORDER,
                MatchOrder.Side.BUY, 6500012L, 3L, 1700000000000L);
    }
}
