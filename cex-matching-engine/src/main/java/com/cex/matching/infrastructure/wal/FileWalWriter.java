package com.cex.matching.infrastructure.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** 基于单个物理文件的 WAL 写入器。 */
public final class FileWalWriter implements WalWriter {
    private final WalCodec codec;
    private final FileChannel channel;
    private long writtenBytes;
    private boolean closed;
    private boolean failed;

    /**
     * 创建追加模式的 WAL 文件写入器。
     *
     * @param path WAL 文件路径
     * @param codec WAL 编解码器
     */
    public FileWalWriter(Path path, WalCodec codec) {
        Path walPath = Objects.requireNonNull(path, "WAL 文件路径不能为空");
        this.codec = Objects.requireNonNull(codec, "WAL 编解码器不能为空");
        try {
            Path parent = walPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            channel = FileChannel.open(walPath, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            writtenBytes = channel.size();
        } catch (IOException e) {
            throw new WalException("打开 WAL 文件失败", e);
        }
    }

    /**
     * 使用受控文件通道创建 WAL 写入器，供同包测试制造真实 I/O 失败。
     *
     * @param path WAL 文件路径
     * @param codec WAL 编解码器
     * @param channel 已打开的文件通道
     */
    FileWalWriter(Path path, WalCodec codec, FileChannel channel) {
        Objects.requireNonNull(path, "WAL 文件路径不能为空");
        this.codec = Objects.requireNonNull(codec, "WAL 编解码器不能为空");
        this.channel = Objects.requireNonNull(channel, "WAL 文件通道不能为空");
        try {
            writtenBytes = channel.size();
        } catch (IOException e) {
            throw new WalException("读取 WAL 文件大小失败", e);
        }
    }

    /**
     * 追加并强制刷盘一条 WAL 记录。
     *
     * @param record 待写入记录
     */
    @Override
    public void append(WalRecord record) {
        ensureUsable();
        ByteBuffer bytes = StandardCharsets.UTF_8.encode(codec.encode(record) + "\n");
        int byteCount = bytes.remaining();
        try {
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
            channel.force(true);
            writtenBytes += byteCount;
        } catch (IOException e) {
            failed = true;
            throw new WalException("追加 WAL 记录失败", e);
        }
    }

    /** 强制将已追加数据刷入稳定存储。 */
    @Override
    public void flush() {
        ensureUsable();
        try {
            channel.force(true);
        } catch (IOException e) {
            failed = true;
            throw new WalException("刷盘 WAL 文件失败", e);
        }
    }

    /**
     * 返回成功刷盘后的累计文件字节数。
     *
     * @return 已确认写入的文件字节数
     */
    @Override
    public long writtenBytes() {
        ensureHealthy();
        return writtenBytes;
    }

    /** 关闭文件通道；失败状态下不再尝试额外刷盘。 */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        WalException failure = null;
        if (!failed) {
            try {
                channel.force(true);
            } catch (IOException e) {
                failed = true;
                failure = new WalException("刷盘 WAL 文件失败", e);
            }
        }
        try {
            channel.close();
        } catch (IOException e) {
            WalException closeFailure = new WalException("关闭 WAL 文件失败", e);
            if (failure != null) {
                failure.addSuppressed(closeFailure);
            } else {
                failure = closeFailure;
            }
        } finally {
            closed = true;
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** 确保写入器仍处于可写且未失败状态。 */
    private void ensureUsable() {
        if (closed) {
            throw new WalException("WAL 写入器已关闭");
        }
        ensureHealthy();
    }

    /** 确保写入器未进入不可恢复的 I/O 失败状态。 */
    private void ensureHealthy() {
        if (failed) {
            throw new WalException("WAL 写入器已处于失败状态");
        }
    }
}
