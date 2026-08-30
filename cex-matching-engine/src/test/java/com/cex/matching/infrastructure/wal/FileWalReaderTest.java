package com.cex.matching.infrastructure.wal;

import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileWalReaderTest {

    @TempDir
    Path tempDir;

    @Test
    void readsValidRecordsBeforeIncompleteUnterminatedTail() throws Exception {
        WalCodec codec = new WalCodec();
        Path file = tempDir.resolve("wal-000001.log");
        Files.writeString(file, codec.encode(record(1L)) + "\n" + codec.encode(record(2L))
                + "\n{\"sequence\":3", StandardCharsets.UTF_8);

        WalReadResult result = new FileWalReader(tempDir, codec).readFile(file);

        assertThat(result.records()).extracting(WalRecord::sequence).containsExactly(1L, 2L);
        assertThat(result.incompleteTail()).isTrue();
    }

    @Test
    void rejectsCorruptionBeforeLastPhysicalRecord() throws Exception {
        WalCodec codec = new WalCodec();
        Path file = tempDir.resolve("wal-000001.log");
        Files.writeString(file, corruptChecksum(codec.encode(record(1L))) + "\n" + codec.encode(record(2L))
                + "\n", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> new FileWalReader(tempDir, codec).readFile(file))
                .isInstanceOf(WalCorruptionException.class);
    }

    @Test
    void mergesFilesByNumberAndRejectsNonIncreasingSequences() throws Exception {
        WalCodec codec = new WalCodec();
        Path symbolDirectory = tempDir.resolve("BTCUSDT");
        Files.createDirectories(symbolDirectory);
        Files.writeString(symbolDirectory.resolve("wal-000002.log"), codec.encode(record(2L)) + "\n",
                StandardCharsets.UTF_8);
        Files.writeString(symbolDirectory.resolve("wal-000001.log"), codec.encode(record(1L)) + "\n",
                StandardCharsets.UTF_8);

        WalReadResult result = new FileWalReader(tempDir, codec).read("BTCUSDT");

        assertThat(result.records()).extracting(WalRecord::sequence).containsExactly(1L, 2L);
        Files.writeString(symbolDirectory.resolve("wal-000002.log"), codec.encode(record(1L)) + "\n",
                StandardCharsets.UTF_8);

        assertThatThrownBy(() -> new FileWalReader(tempDir, codec).read("BTCUSDT"))
                .isInstanceOf(WalCorruptionException.class);
    }

    @Test
    void acceptsIncompleteTailWhenLaterWalFileIsEmpty() throws Exception {
        WalCodec codec = new WalCodec();
        Path symbolDirectory = tempDir.resolve("BTCUSDT");
        Files.createDirectories(symbolDirectory);
        Files.writeString(symbolDirectory.resolve("wal-000001.log"), codec.encode(record(1L)) + "\n{\"sequence\":2",
                StandardCharsets.UTF_8);
        Files.writeString(symbolDirectory.resolve("wal-000002.log"), "", StandardCharsets.UTF_8);

        WalReadResult result = new FileWalReader(tempDir, codec).read("BTCUSDT");

        assertThat(result.records()).extracting(WalRecord::sequence).containsExactly(1L);
        assertThat(result.incompleteTail()).isTrue();
    }

    @Test
    void ignoresLastValidRecordWithoutTerminatingNewline() throws Exception {
        WalCodec codec = new WalCodec();
        Path file = tempDir.resolve("wal-000001.log");
        Files.writeString(file, codec.encode(record(1L)) + "\n" + codec.encode(record(2L)), StandardCharsets.UTF_8);

        WalReadResult result = new FileWalReader(tempDir, codec).readFile(file);

        assertThat(result.records()).extracting(WalRecord::sequence).containsExactly(1L);
        assertThat(result.incompleteTail()).isTrue();
    }

    @Test
    void ignoresLastCorruptRecordWithTerminatingNewline() throws Exception {
        WalCodec codec = new WalCodec();
        Path file = tempDir.resolve("wal-000001.log");
        Files.writeString(file, codec.encode(record(1L)) + "\n" + corruptChecksum(codec.encode(record(2L))) + "\n",
                StandardCharsets.UTF_8);

        WalReadResult result = new FileWalReader(tempDir, codec).readFile(file);

        assertThat(result.records()).extracting(WalRecord::sequence).containsExactly(1L);
        assertThat(result.incompleteTail()).isTrue();
    }

    @Test
    void rejectsDecreasingSequenceAcrossWalFiles() throws Exception {
        WalCodec codec = new WalCodec();
        Path symbolDirectory = tempDir.resolve("BTCUSDT");
        Files.createDirectories(symbolDirectory);
        Files.writeString(symbolDirectory.resolve("wal-000001.log"), codec.encode(record(2L)) + "\n",
                StandardCharsets.UTF_8);
        Files.writeString(symbolDirectory.resolve("wal-000002.log"), codec.encode(record(1L)) + "\n",
                StandardCharsets.UTF_8);

        assertThatThrownBy(() -> new FileWalReader(tempDir, codec).read("BTCUSDT"))
                .isInstanceOf(WalCorruptionException.class);
    }

    /**
     * 验证读取端拒绝路径逃逸、绝对路径、分隔符及非规范大小写交易对。
     */
    @Test
    void rejectsUnsafeOrNonCanonicalSymbolsBeforeReading() {
        FileWalReader reader = new FileWalReader(tempDir, new WalCodec());
        List<String> unsafeSymbols = List.of(".", "..", "../escape", "BTC/USDT", "BTC\\USDT",
                "btc_usdt", "Btc_Usdt", tempDir.resolve("outside").toAbsolutePath().toString());

        for (String unsafeSymbol : unsafeSymbols) {
            assertThatThrownBy(() -> reader.read(unsafeSymbol))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static String corruptChecksum(String encoded) {
        return encoded.replaceFirst("\\\"checksum\\\":\\d+", "\\\"checksum\\\":0");
    }

    private static WalRecord record(long sequence) {
        return new WalRecord(sequence, "cmd-" + sequence, "ord-" + sequence,
                "usr-" + sequence, "BTCUSDT", CommandType.NEW_ORDER, MatchOrder.Side.BUY,
                6500012L, 3L, 1700000000000L, 0L);
    }
}
