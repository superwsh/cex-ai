package com.cex.matching.infrastructure.snapshot;

import com.cex.matching.domain.snapshot.MatchingSnapshot;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** 读取单个快照并在目录中回退至最新有效快照。 */
public final class SnapshotReader {
    private static final Pattern FILE = Pattern.compile("snapshot-(\\d+)\\.json");
    private final SnapshotCodec codec;
    /** @param codec 快照 JSON 编解码器 */
    public SnapshotReader(SnapshotCodec codec) { this.codec = codec; }
    /** @param path 快照文件 @return 已解析快照 */
    public MatchingSnapshot read(Path path) { try { return codec.decode(Files.readAllBytes(path)); } catch (IOException e) { throw new SnapshotException("读取快照失败", e); } }
    /** @param directory 交易对快照目录 @return 最新有效快照 */
    public Optional<MatchingSnapshot> readLatest(Path directory) { if (Files.notExists(directory)) return Optional.empty(); try (Stream<Path> paths = Files.list(directory)) { return paths.filter(path -> FILE.matcher(path.getFileName().toString()).matches()).sorted(Comparator.comparingLong(this::sequence).reversed()).map(this::tryRead).flatMap(Optional::stream).findFirst(); } catch (IOException e) { throw new SnapshotException("扫描快照目录失败", e); } }
    /** @param path 快照文件 @return 文件名中的序列 */
    private long sequence(Path path) { var matcher = FILE.matcher(path.getFileName().toString()); matcher.matches(); return Long.parseLong(matcher.group(1)); }
    /** @param path 快照文件 @return 可读取快照或空 */
    private Optional<MatchingSnapshot> tryRead(Path path) { try { return Optional.of(read(path)); } catch (SnapshotException e) { return Optional.empty(); } }
}
