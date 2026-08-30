package com.cex.matching.infrastructure.wal;

import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WalManagerTest {

    @TempDir(factory = WalTestTempDirFactory.class)
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
        List<String> unsafeSymbols = List.of("../escape", "BTC/USDT", "BTC\\USDT", "btc_usdt", "Btc_Usdt",
                tempDir.resolve("outside").toAbsolutePath().toString());
        WalManager.WriterFactory factory = (path, codec) -> new RecordingWriter();
        WalManager manager = new WalManager(tempDir, 10_000L, new WalCodec(), factory);

        for (String unsafeSymbol : unsafeSymbols) {
            assertThatThrownBy(() -> manager.append(record(1L, unsafeSymbol)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /**
     * 验证写入端拒绝解析后位于 WAL 根目录外的交易对链接目录。
     */
    @Test
    void rejectsSymbolicLinkDirectoryOutsideWalRootWhenWriting() throws Exception {
        Path root = tempDir.resolve("root");
        Path outside = tempDir.resolve("outside");
        Files.createDirectories(root);
        Files.createDirectories(outside);
        createSymbolicLinkOrSkip(root.resolve("BTCUSDT"), outside);

        try (WalManager manager = new WalManager(root, 10_000L, new WalCodec())) {
            assertThatThrownBy(() -> manager.append(record(1L, "BTCUSDT")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("WAL 根目录");
        }
        assertThat(Files.exists(outside.resolve("wal-000001.log"))).isFalse();
    }

    /**
     * 验证已存在的链接属于测试构造错误，不能被误判为环境权限不足而跳过。
     */
    @Test
    void throwsWhenSymbolicLinkAlreadyExists() throws Exception {
        Path link = tempDir.resolve("existing-link");
        Path target = tempDir.resolve("target");
        try (MockedStatic<Files> files = Mockito.mockStatic(Files.class)) {
            files.when(() -> Files.createSymbolicLink(link, target))
                    .thenThrow(new java.nio.file.FileAlreadyExistsException(link.toString()));

            assertThatThrownBy(() -> createSymbolicLinkOrSkip(link, target))
                    .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        }
    }

    /**
     * 验证其他 IO 失败必须暴露，不能让链接安全测试静默跳过。
     */
    @Test
    void throwsWhenSymbolicLinkCreationFailsForOtherIOException() throws Exception {
        Path link = tempDir.resolve("failed-link");
        Path target = tempDir.resolve("target");
        try (MockedStatic<Files> files = Mockito.mockStatic(Files.class)) {
            files.when(() -> Files.createSymbolicLink(link, target))
                    .thenThrow(new java.io.IOException("模拟路径 IO 失败"));

            assertThatThrownBy(() -> createSymbolicLinkOrSkip(link, target))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessage("模拟路径 IO 失败");
        }
    }

    /**
     * 验证文件系统不支持符号链接时必须暴露异常，不能静默跳过。
     */
    @Test
    void throwsWhenSymbolicLinksAreUnsupported() {
        Path link = tempDir.resolve("unsupported-link");
        Path target = tempDir.resolve("target");
        try (MockedStatic<Files> files = Mockito.mockStatic(Files.class)) {
            files.when(() -> Files.createSymbolicLink(link, target))
                    .thenThrow(new UnsupportedOperationException("模拟文件系统不支持链接"));

            assertThatThrownBy(() -> createSymbolicLinkOrSkip(link, target))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    /**
     * 验证安全策略拒绝创建链接时必须暴露异常，不能静默跳过。
     */
    @Test
    void throwsWhenSymbolicLinkCreationIsRejectedBySecurityManager() {
        Path link = tempDir.resolve("security-link");
        Path target = tempDir.resolve("target");
        try (MockedStatic<Files> files = Mockito.mockStatic(Files.class)) {
            files.when(() -> Files.createSymbolicLink(link, target))
                    .thenThrow(new SecurityException("模拟安全策略拒绝"));

            assertThatThrownBy(() -> createSymbolicLinkOrSkip(link, target))
                    .isInstanceOf(SecurityException.class);
        }
    }

    /**
     * 验证重新打开管理器后会续写已有的最大编号 WAL 文件。
     */
    @Test
    void appendsToLargestExistingFileAfterManagerRestart() throws Exception {
        WalCodec codec = new WalCodec();
        Path symbolDirectory = tempDir.resolve("BTC_USDT");
        writeRecord(symbolDirectory.resolve("wal-000001.log"), codec, record(1L, "BTC_USDT"));
        writeRecord(symbolDirectory.resolve("wal-000002.log"), codec, record(2L, "BTC_USDT"));
        try (WalManager manager = new WalManager(tempDir, 10_000L, codec)) {
            manager.append(record(3L, "BTC_USDT"));
        }

        assertThat(Files.readAllLines(symbolDirectory.resolve("wal-000001.log"))).hasSize(1);
        assertThat(Files.readAllLines(symbolDirectory.resolve("wal-000002.log"))).hasSize(2);
        assertThat(new FileWalReader(tempDir, codec).read("BTC_USDT").records())
                .extracting(WalRecord::sequence)
                .containsExactly(1L, 2L, 3L);
    }

    /**
     * 验证重启时截断半写 JSON 尾部，再从最后一个完整换行记录后继续追加。
     */
    @Test
    void truncatesHalfWrittenTailBeforeAppendingAfterRestart() throws Exception {
        WalCodec codec = new WalCodec();
        Path file = tempDir.resolve("BTC_USDT/wal-000001.log");
        Files.createDirectories(file.getParent());
        Files.writeString(file, codec.encode(record(1L, "BTC_USDT")) + "\n{\"sequence\":2",
                StandardCharsets.UTF_8);

        try (WalManager manager = new WalManager(tempDir, 10_000L, codec)) {
            manager.append(record(2L, "BTC_USDT"));
        }

        assertThat(new FileWalReader(tempDir, codec).read("BTC_USDT").records())
                .extracting(WalRecord::sequence)
                .containsExactly(1L, 2L);
    }

    /**
     * 验证重启时截断最后一条校验失败记录，再保留既有记录并追加新记录。
     */
    @Test
    void truncatesChecksumFailedTailBeforeAppendingAfterRestart() throws Exception {
        WalCodec codec = new WalCodec();
        Path file = tempDir.resolve("BTC_USDT/wal-000001.log");
        Files.createDirectories(file.getParent());
        String corruptTail = codec.encode(record(2L, "BTC_USDT"))
                .replaceFirst("\\\"checksum\\\":\\d+", "\\\"checksum\\\":0");
        Files.writeString(file, codec.encode(record(1L, "BTC_USDT")) + "\n" + corruptTail + "\n",
                StandardCharsets.UTF_8);

        try (WalManager manager = new WalManager(tempDir, 10_000L, codec)) {
            manager.append(record(2L, "BTC_USDT"));
        }

        assertThat(new FileWalReader(tempDir, codec).read("BTC_USDT").records())
                .extracting(WalRecord::sequence)
                .containsExactly(1L, 2L);
    }

    /**
     * 验证最大编号文件中的中间损坏会阻止重启续写。
     */
    @Test
    void rejectsRestartAppendWhenLatestFileContainsMiddleCorruption() throws Exception {
        WalCodec codec = new WalCodec();
        Path file = tempDir.resolve("BTC_USDT/wal-000001.log");
        Files.createDirectories(file.getParent());
        String corruptMiddle = codec.encode(record(1L, "BTC_USDT"))
                .replaceFirst("\\\"checksum\\\":\\d+", "\\\"checksum\\\":0");
        Files.writeString(file, corruptMiddle + "\n" + codec.encode(record(2L, "BTC_USDT")) + "\n",
                StandardCharsets.UTF_8);

        try (WalManager manager = new WalManager(tempDir, 10_000L, codec)) {
            assertThatThrownBy(() -> manager.append(record(3L, "BTC_USDT")))
                    .isInstanceOf(WalCorruptionException.class);
        }

        assertThat(Files.readString(file, StandardCharsets.UTF_8))
                .doesNotContain("\"sequence\":3");
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
        WalRecord firstRecord = record(1L, "BTC_USDT");
        WalRecord secondRecord = record(2L, "BTC_USDT");
        long threshold = encodedLineBytes(codec, firstRecord) + encodedLineBytes(codec, secondRecord);
        try (WalManager manager = new WalManager(tempDir, threshold, codec)) {
            manager.append(firstRecord);
            manager.append(secondRecord);
        }

        assertThat(Files.exists(tempDir.resolve("BTC_USDT/wal-000002.log"))).isFalse();
    }

    /**
     * 验证相对根目录规范化为绝对路径后，正常交易对仍可写入。
     */
    @Test
    void acceptsNormalSymbolWhenRootIsCurrentRelativeDirectory() throws Exception {
        Path relativeRoot = Path.of("target", "relative-wal-" + System.nanoTime());
        RecordingWriter writer = new RecordingWriter();
        try (WalManager manager = new WalManager(relativeRoot, 10_000L, new WalCodec(),
                (path, codec) -> writer)) {
            manager.append(record(1L, "BTC_USDT"));
        }

        assertThat(writer.closeCount).isEqualTo(1);
        Files.deleteIfExists(relativeRoot.resolve("BTC_USDT"));
        Files.deleteIfExists(relativeRoot);
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
     * 验证底层追加失败后管理器不会通过文件滚动绕过失败写入器。
     */
    @Test
    void doesNotRollPastFailedFileWriter() throws Exception {
        WalCodec codec = new WalCodec();
        WalRecord firstRecord = record(1L, "BTCUSDT");
        WalRecord failedRecord = record(2L, "BTCUSDT");
        WalRecord oversizedRecord = recordWithCommandId(3L, "BTCUSDT", "x".repeat(1_024));
        long threshold = encodedLineBytes(codec, firstRecord) + encodedLineBytes(codec, failedRecord);
        AtomicInteger openCount = new AtomicInteger();
        AtomicReference<FileChannel> firstChannel = new AtomicReference<>();
        WalManager.WriterFactory factory = (path, walCodec) -> {
            int currentOpenCount = openCount.incrementAndGet();
            if (currentOpenCount > 1) {
                return new RecordingWriter();
            }
            try {
                Files.createDirectories(path.getParent());
                FileChannel delegate = FileChannel.open(path, java.nio.file.StandardOpenOption.CREATE,
                        java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.APPEND);
                firstChannel.set(delegate);
                return new FileWalWriter(path, walCodec, delegate);
            } catch (java.io.IOException e) {
                throw new AssertionError("构造受控 WAL 文件通道失败", e);
            }
        };

        try (WalManager manager = new WalManager(tempDir, threshold, codec, factory)) {
            manager.append(firstRecord);
            firstChannel.get().close();
            assertThatThrownBy(() -> manager.append(failedRecord))
                    .isInstanceOf(WalException.class)
                    .hasCauseInstanceOf(java.io.IOException.class);

            assertThatThrownBy(() -> manager.append(oversizedRecord))
                    .isInstanceOf(WalException.class)
                    .hasMessageContaining("失败状态");
        }
        assertThat(openCount).hasValue(1);
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
     * 将固定记录写入指定 WAL 文件以构造重启场景。
     *
     * @param file WAL 文件
     * @param codec WAL 编解码器
     * @param record 要写入的记录
     */
    private static void writeRecord(Path file, WalCodec codec, WalRecord record) {
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, codec.encode(record) + "\n", StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new AssertionError("构造 WAL 测试文件失败", e);
        }
    }

    /**
     * 构造用于真实 WAL 文件读写的固定记录。
     *
     * @param sequence WAL 序列号
     * @param symbol 交易对
     * @return 固定字段的 WAL 记录
     */
    private static WalRecord record(long sequence, String symbol) {
        return recordWithCommandId(sequence, symbol, "cmd-" + symbol + "-" + sequence);
    }

    /**
     * 构造带指定命令编号的 WAL 记录，用于控制编码行大小。
     *
     * @param sequence WAL 序列号
     * @param symbol 交易对
     * @param commandId 命令编号
     * @return 固定字段的 WAL 记录
     */
    private static WalRecord recordWithCommandId(long sequence, String symbol, String commandId) {
        return new WalRecord(sequence, commandId,
                String.valueOf(sequence), "202", symbol,
                CommandType.NEW_ORDER, MatchOrder.Side.BUY, 6500012L, 3L,
                1700000000000L, 0L);
    }

    /**
     * 创建目录符号链接；仅在权限不足或 Windows 缺少创建链接特权时跳过链接测试。
     *
     * @param link 待创建的链接
     * @param target 链接目标目录
     */
    private static void createSymbolicLinkOrSkip(Path link, Path target) throws java.io.IOException {
        try {
            Files.createSymbolicLink(link, target);
        } catch (AccessDeniedException e) {
            skipForSymbolicLinkPermission(e);
        } catch (java.io.IOException e) {
            if (isWindowsSymbolicLinkPrivilegeFailure(e)) {
                skipForSymbolicLinkPermission(e);
            }
            throw e;
        }
    }

    /**
     * 判断是否为 Windows 创建符号链接时缺少特权的已知失败。
     *
     * @param exception 创建链接时发生的 IO 异常
     * @return 是否应将该异常视为环境限制
     */
    private static boolean isWindowsSymbolicLinkPrivilegeFailure(java.io.IOException exception) {
        String message = exception.getMessage();
        if (!System.getProperty("os.name", "").startsWith("Windows") || message == null) {
            return false;
        }
        String normalizedMessage = message.toLowerCase(Locale.ROOT);
        return normalizedMessage.contains("required privilege is not held by the client")
                || normalizedMessage.contains("客户端没有所需的特权");
    }

    /**
     * 因环境权限不足中止链接测试。
     *
     * @param exception 创建链接时发生的权限异常
     */
    private static void skipForSymbolicLinkPermission(Exception exception) {
        Assumptions.assumeTrue(false, "当前环境没有创建目录符号链接的权限: " + exception.getMessage());
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
