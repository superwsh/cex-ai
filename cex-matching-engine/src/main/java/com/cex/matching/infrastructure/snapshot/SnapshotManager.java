package com.cex.matching.infrastructure.snapshot;

import com.cex.matching.domain.snapshot.MatchingSnapshot;
import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Pattern;

/** 按交易对隔离并管理版本化订单簿快照。 */
public final class SnapshotManager {
    private static final Pattern SYMBOL = Pattern.compile("[A-Z0-9_-]+");
    private final Path root;
    private final SnapshotWriter writer;
    private final SnapshotReader reader;
    /** @param root 快照根目录 @param writer 原子写入器 @param reader 回退读取器 */
    public SnapshotManager(Path root, SnapshotWriter writer, SnapshotReader reader) { this.root = root.toAbsolutePath().normalize(); this.writer = writer; this.reader = reader; }
    /** @param snapshot 待保存快照 */
    public void save(MatchingSnapshot snapshot) { writer.write(file(snapshot.symbol(), snapshot.lastSequence()), snapshot); }
    /** @param symbol 交易对 @return 最新有效快照 */
    public Optional<MatchingSnapshot> loadLatest(String symbol) { return reader.readLatest(directory(symbol)); }
    /** @param symbol 交易对 @param sequence 快照序列 @return 快照文件路径 */
    private Path file(String symbol, long sequence) { return directory(symbol).resolve("snapshot-" + sequence + ".json"); }
    /** @param symbol 交易对 @return 已校验目录 */
    private Path directory(String symbol) { if (symbol == null || !SYMBOL.matcher(symbol).matches()) throw new IllegalArgumentException("交易对目录名不合法"); return root.resolve(symbol).normalize(); }
}
