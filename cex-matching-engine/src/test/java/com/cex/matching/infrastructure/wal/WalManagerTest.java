package com.cex.matching.infrastructure.wal;

import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WalManagerTest {

    @TempDir
    Path tempDir;

    /**
     * 验证超过阈值时按完整记录滚动文件，并能按顺序读回记录。
     */
    @Test
    void rollsFilesWhenCompleteNextLineWouldExceedThreshold() throws Exception {
        WalCodec codec = new WalCodec();
        try (WalManager manager = new WalManager(tempDir, 1L, codec)) {
            manager.append(record(1L, "BTC_USDT"));
            manager.append(record(2L, "BTC_USDT"));
        }

        Path symbolDirectory = tempDir.resolve("BTC_USDT");
        assertThat(Files.exists(symbolDirectory.resolve("wal-000001.log"))).isTrue();
        assertThat(Files.exists(symbolDirectory.resolve("wal-000002.log"))).isTrue();
        assertThat(new FileWalReader(tempDir, codec).read("BTC_USDT").records())
                .extracting(WalRecord::sequence)
                .containsExactly(1L, 2L);
    }

    /**
     * 验证不同交易对使用彼此独立的 WAL 目录。
     */
    @Test
    void isolatesWalFilesBySymbol() {
        WalCodec codec = new WalCodec();
        try (WalManager manager = new WalManager(tempDir, 10_000L, codec)) {
            manager.append(record(1L, "BTC_USDT"));
            manager.append(record(1L, "ETH_USDT"));
        }

        assertThat(new FileWalReader(tempDir, codec).read("BTC_USDT").records())
                .extracting(WalRecord::sequence)
                .containsExactly(1L);
        assertThat(new FileWalReader(tempDir, codec).read("ETH_USDT").records())
                .extracting(WalRecord::sequence)
                .containsExactly(1L);
    }

    /**
     * 验证空 WAL 文件允许写入超过阈值的一条完整记录。
     */
    @Test
    void keepsOversizedRecordWholeWhenFileIsEmpty() {
        WalCodec codec = new WalCodec();
        try (WalManager manager = new WalManager(tempDir, 1L, codec)) {
            manager.append(record(1L, "BTC_USDT"));
        }

        Path symbolDirectory = tempDir.resolve("BTC_USDT");
        assertThat(Files.exists(symbolDirectory.resolve("wal-000001.log"))).isTrue();
        assertThat(Files.exists(symbolDirectory.resolve("wal-000002.log"))).isFalse();
        assertThat(new FileWalReader(tempDir, codec).read("BTC_USDT").records())
                .extracting(WalRecord::sequence)
                .containsExactly(1L);
    }

    /**
     * 验证关闭后拒绝继续写入和刷盘，并校验非法阈值。
     */
    @Test
    void rejectsOperationsAfterCloseAndRejectsNonPositiveThreshold() {
        WalManager manager = new WalManager(tempDir, 10_000L, new WalCodec());
        manager.close();

        assertThatThrownBy(() -> manager.append(record(1L, "BTC_USDT")))
                .isInstanceOf(WalException.class);
        assertThatThrownBy(manager::flush)
                .isInstanceOf(WalException.class);
        assertThatThrownBy(() -> new WalManager(tempDir, 0L, new WalCodec()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * 验证交易对不能逃逸 WAL 根目录或形成嵌套目录。
     */
    @Test
    void rejectsUnsafeSymbolDirectorySegments() {
        List<String> unsafeSymbols = List.of("../escape", "BTC/USDT", "BTC\\USDT",
                tempDir.resolve("outside").toAbsolutePath().toString());
        WalManager.WriterFactory factory = (path, codec) -> new RecordingWriter();
        WalManager manager = new WalManager(tempDir, 10_000L, new WalCodec(), factory);

        for (String unsafeSymbol : unsafeSymbols) {
            assertThatThrownBy(() -> manager.append(record(1L, unsafeSymbol)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /**
     * 验证重新打开管理器后会续写已有的最大编号 WAL 文件。
     */
    @Test
    void appendsToLargestExistingFileAfterManagerRestart() {
        WalCodec codec = new WalCodec();
        try (WalManager manager = new WalManager(tempDir, 10_000L, codec)) {
            manager.append(record(1L, "BTC_USDT"));
        }
        try (WalManager manager = new WalManager(tempDir, 10_000L, codec)) {
            manager.append(record(2L, "BTC_USDT"));
        }

        assertThat(Files.exists(tempDir.resolve("BTC_USDT/wal-000002.log"))).isFalse();
        assertThat(new FileWalReader(tempDir, codec).read("BTC_USDT").records())
                .extracting(WalRecord::sequence)
                .containsExactly(1L, 2L);
    }

    /**
     * 验证重新打开后，已达到阈值的最大编号文件会在追加前滚动。
     */
    @Test
    void rollsLargestExistingFileWhenItHasReachedThreshold() throws Exception {
        WalCodec codec = new WalCodec();
        long lineBytes = encodedLineBytes(codec, record(1L, "BTC_USDT"));
        try (WalManager manager = new WalManager(tempDir, lineBytes, codec)) {
            manager.append(record(1L, "BTC_USDT"));
        }
        try (WalManager manager = new WalManager(tempDir, lineBytes, codec)) {
            manager.append(record(2L, "BTC_USDT"));
        }

        assertThat(Files.exists(tempDir.resolve("BTC_USDT/wal-000002.log"))).isTrue();
        assertThat(new FileWalReader(tempDir, codec).read("BTC_USDT").records())
                .extracting(WalRecord::sequence)
                .containsExactly(1L, 2L);
    }

    /**
     * 验证追加后字节数恰好等于阈值时不滚动。
     */
    @Test
    void doesNotRollWhenCompleteLinesExactlyReachThreshold() {
        WalCodec codec = new WalCodec();
        long lineBytes = encodedLineBytes(codec, record(1L, "BTC_USDT"));
        try (WalManager manager = new WalManager(tempDir, lineBytes * 2L, codec)) {
            manager.append(record(1L, "BTC_USDT"));
            manager.append(record(2L, "BTC_USDT"));
        }

        assertThat(Files.exists(tempDir.resolve("BTC_USDT/wal-000002.log"))).isFalse();
    }

    /**
     * 验证最大编号文件需要滚动时拒绝创建超出范围的文件。
     */
    @Test
    void rejectsRollBeyondMaximumFileNumber() throws Exception {
        WalCodec codec = new WalCodec();
        Path symbolDirectory = tempDir.resolve("BTC_USDT");
        Files.createDirectories(symbolDirectory);
        Files.writeString(symbolDirectory.resolve("wal-999999.log"), codec.encode(record(1L, "BTC_USDT")) + "\n",
                StandardCharsets.UTF_8);

        try (WalManager manager = new WalManager(tempDir, 1L, codec)) {
            assertThatThrownBy(() -> manager.append(record(2L, "BTC_USDT")))
                    .isInstanceOf(WalException.class)
                    .hasMessageContaining("999999");
        }
        assertThat(Files.exists(symbolDirectory.resolve("wal-1000000.log"))).isFalse();
    }

    /**
     * 验证重复关闭不会重复关闭底层写入器。
     */
    @Test
    void closesOnlyOnceWhenCloseIsCalledRepeatedly() {
        RecordingWriter writer = new RecordingWriter();
        WalManager manager = new WalManager(tempDir, 10_000L, new WalCodec(), (path, codec) -> writer);
        manager.append(record(1L, "BTC_USDT"));

        manager.close();
        manager.close();

        assertThat(writer.closeCount).isEqualTo(1);
    }

    /**
     * 验证关闭一个写入器失败时仍继续关闭其他写入器。
     */
    @Test
    void closesRemainingWritersWhenOneWriterCloseFails() {
        RecordingWriter successfulWriter = new RecordingWriter();
        RecordingWriter failingWriter = new FailingCloseWriter();
        List<RecordingWriter> writers = new ArrayList<>();
        WalManager.WriterFactory factory = (path, codec) -> {
            RecordingWriter writer = path.getParent().getFileName().toString().equals("BTC_USDT")
                    ? failingWriter : successfulWriter;
            writers.add(writer);
            return writer;
        };
        WalManager manager = new WalManager(tempDir, 10_000L, new WalCodec(), factory);
        manager.append(record(1L, "BTC_USDT"));
        manager.append(record(1L, "ETH_USDT"));

        assertThatThrownBy(manager::close).isInstanceOf(WalException.class);
        assertThat(writers).hasSize(2);
        assertThat(successfulWriter.closeCount).isEqualTo(1);
        assertThat(failingWriter.closeCount).isEqualTo(1);
    }

    /**
     * 计算记录写入 WAL 时完整行的 UTF-8 字节数。
     *
     * @param codec WAL 编解码器
     * @param record WAL 记录
     * @return 编码结果连同换行符的字节数
     */
    private static long encodedLineBytes(WalCodec codec, WalRecord record) {
        return codec.encode(record).getBytes(StandardCharsets.UTF_8).length + 1L;
    }

    /**
     * 构造用于真实 WAL 文件读写的固定记录。
     *
     * @param sequence WAL 序列号
     * @param symbol 交易对
     * @return 固定字段的 WAL 记录
     */
    private static WalRecord record(long sequence, String symbol) {
        return new WalRecord(sequence, "cmd-" + symbol + "-" + sequence,
                "ord-" + symbol + "-" + sequence, "usr-" + sequence, symbol,
                CommandType.NEW_ORDER, MatchOrder.Side.BUY, 6500012L, 3L,
                1700000000000L, 0L);
    }

    /** 用于验证管理器资源生命周期的内存写入器。 */
    private static class RecordingWriter implements WalWriter {
        private int closeCount;

        /**
         * 接受测试记录，不进行外部 IO。
         *
         * @param record WAL 记录
         */
        @Override
        public void append(WalRecord record) {
        }

        /** 刷盘内存写入器。 */
        @Override
        public void flush() {
        }

        /**
         * 返回内存写入器当前字节数。
         *
         * @return 已写入字节数
         */
        @Override
        public long writtenBytes() {
            return 0L;
        }

        /** 记录写入器被关闭的次数。 */
        @Override
        public void close() {
            closeCount++;
        }
    }

    /** 关闭时固定抛出异常的受控写入器。 */
    private static final class FailingCloseWriter extends RecordingWriter {

        /**
         * 记录关闭次数后模拟关闭失败。
         */
        @Override
        public void close() {
            super.close();
            throw new WalException("模拟关闭失败");
        }
    }
}
