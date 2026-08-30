package com.cex.matching.infrastructure.wal;

import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

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
}
