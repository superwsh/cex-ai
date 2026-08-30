package com.cex.matching.infrastructure.snapshot;

import com.cex.matching.domain.snapshot.MatchingSnapshot;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/** 以临时文件和原子替换方式持久化快照。 */
public final class SnapshotWriter {
    private final SnapshotCodec codec;
    /** @param codec 快照 JSON 编解码器 */
    public SnapshotWriter(SnapshotCodec codec) { this.codec = codec; }
    /** @param target 快照目标文件 @param snapshot 待持久化快照 */
    public void write(Path target, MatchingSnapshot snapshot) {
        Path temp = target.resolveSibling(target.getFileName() + ".tmp");
        try { Files.createDirectories(target.getParent()); try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) { channel.write(ByteBuffer.wrap(codec.encode(snapshot))); channel.force(true); } Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); } catch (IOException e) { throw new SnapshotException("原子写入快照失败", e); }
    }
}
