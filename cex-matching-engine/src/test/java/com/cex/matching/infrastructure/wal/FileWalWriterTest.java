package com.cex.matching.infrastructure.wal;

import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileWalWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void appendsTwoRecordsAsDecodableLines() throws Exception {
        Path path = tempDir.resolve("nested/wal.log");
        WalCodec codec = new WalCodec();

        try (WalWriter writer = new FileWalWriter(path, codec)) {
            writer.append(record(1L));
            writer.append(record(2L));
        }

        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        assertThat(lines).hasSize(2);
        assertThat(codec.decode(lines.get(0)).sequence()).isEqualTo(1L);
        assertThat(codec.decode(lines.get(1)).sequence()).isEqualTo(2L);
    }

    @Test
    void includesExistingFileSizeInWrittenBytes() throws Exception {
        Path path = tempDir.resolve("wal.log");
        byte[] existing = "已有记录\n".getBytes(StandardCharsets.UTF_8);
        Files.write(path, existing);

        try (WalWriter writer = new FileWalWriter(path, new WalCodec())) {
            assertThat(writer.writtenBytes()).isEqualTo(existing.length);
        }
    }

    @Test
    void rejectsAppendAfterClose() {
        Path path = tempDir.resolve("wal.log");
        FileWalWriter writer = new FileWalWriter(path, new WalCodec());
        writer.close();

        assertThatThrownBy(() -> writer.append(record(1L)))
                .isInstanceOf(WalException.class);
    }

    /**
     * 验证追加 I/O 失败后写入器进入失败状态，后续追加和刷盘立即失败。
     */
    @Test
    void rejectsFurtherOperationsAfterAppendIoFailure() throws Exception {
        Path path = tempDir.resolve("append-failure.log");
        FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        FileWalWriter writer = new FileWalWriter(path, new WalCodec(), channel);
        channel.close();

        assertThatThrownBy(() -> writer.append(record(1L)))
                .isInstanceOf(WalException.class)
                .hasCauseInstanceOf(java.io.IOException.class);
        assertThatThrownBy(() -> writer.append(record(2L)))
                .isInstanceOf(WalException.class)
                .hasMessageContaining("失败状态")
                .hasNoCause();
        assertThatThrownBy(writer::flush)
                .isInstanceOf(WalException.class)
                .hasMessageContaining("失败状态")
                .hasNoCause();
        assertThatThrownBy(writer::writtenBytes)
                .isInstanceOf(WalException.class)
                .hasMessageContaining("失败状态")
                .hasNoCause();
    }

    /**
     * 验证刷盘 I/O 失败后写入器进入失败状态，后续刷盘和追加立即失败。
     */
    @Test
    void rejectsFurtherOperationsAfterFlushIoFailure() throws Exception {
        Path path = tempDir.resolve("flush-failure.log");
        FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        FileWalWriter writer = new FileWalWriter(path, new WalCodec(), channel);
        channel.close();

        assertThatThrownBy(writer::flush)
                .isInstanceOf(WalException.class)
                .hasCauseInstanceOf(java.io.IOException.class);
        assertThatThrownBy(writer::flush)
                .isInstanceOf(WalException.class)
                .hasMessageContaining("失败状态")
                .hasNoCause();
        assertThatThrownBy(() -> writer.append(record(1L)))
                .isInstanceOf(WalException.class)
                .hasMessageContaining("失败状态")
                .hasNoCause();
    }

    private static WalRecord record(long sequence) {
        return new WalRecord(sequence, "cmd-" + sequence, "ord-" + sequence,
                "usr-" + sequence, "BTCUSDT", CommandType.NEW_ORDER, MatchOrder.Side.BUY,
                6500012L, 3L, 1700000000000L, 0L);
    }
}
