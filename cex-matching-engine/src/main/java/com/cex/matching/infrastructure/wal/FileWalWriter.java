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

    @Override
    public synchronized void append(WalRecord record) {
        ensureOpen();
        ByteBuffer bytes = StandardCharsets.UTF_8.encode(codec.encode(record) + "\n");
        int byteCount = bytes.remaining();
        try {
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
            channel.force(true);
            writtenBytes += byteCount;
        } catch (IOException e) {
            throw new WalException("追加 WAL 记录失败", e);
        }
    }

    @Override
    public synchronized void flush() {
        ensureOpen();
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new WalException("刷盘 WAL 文件失败", e);
        }
    }

    @Override
    public synchronized long writtenBytes() {
        return writtenBytes;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        WalException failure = null;
        try {
            channel.force(true);
        } catch (IOException e) {
            failure = new WalException("刷盘 WAL 文件失败", e);
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

    private void ensureOpen() {
        if (closed) {
            throw new WalException("WAL 写入器已关闭");
        }
    }
}
