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
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileWalReaderTest {

    @TempDir(factory = WalTestTempDirFactory.class)
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

    /**
     * 验证读取端拒绝解析后位于 WAL 根目录外的交易对链接目录。
     */
    @Test
    void rejectsSymbolicLinkDirectoryOutsideWalRootWhenReading() throws Exception {
        Path root = tempDir.resolve("root");
        Path outside = tempDir.resolve("outside");
        Files.createDirectories(root);
        Files.createDirectories(outside);
        createSymbolicLinkOrSkip(root.resolve("BTCUSDT"), outside);

        assertThatThrownBy(() -> new FileWalReader(root, new WalCodec()).read("BTCUSDT"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("WAL 根目录");
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

    private static String corruptChecksum(String encoded) {
        return encoded.replaceFirst("\\\"checksum\\\":\\d+", "\\\"checksum\\\":0");
    }

    private static WalRecord record(long sequence) {
        return new WalRecord(sequence, "cmd-" + sequence, "ord-" + sequence,
                "usr-" + sequence, "BTCUSDT", CommandType.NEW_ORDER, MatchOrder.Side.BUY,
                6500012L, 3L, 1700000000000L, 0L);
    }
}
